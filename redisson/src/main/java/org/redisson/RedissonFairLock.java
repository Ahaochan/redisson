/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>fair</b> locking so it guarantees an acquire order by threads.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonFairLock extends RedissonLock implements RLock {

    private final long threadWaitTime;
    private final CommandAsyncExecutor commandExecutor;
    private final String threadsQueueName;
    private final String timeoutSetName;

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name) {
        this(commandExecutor, name, 60000*5);
    }

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name, long threadWaitTime) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.threadWaitTime = threadWaitTime;
        threadsQueueName = prefixName("redisson_lock_queue", name);
        timeoutSetName = prefixName("redisson_lock_timeout", name);
    }

    @Override
    protected CompletableFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected void unsubscribe(RedissonLockEntry entry, long threadId) {
        pubSub.unsubscribe(entry, getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected CompletableFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        long wait = threadWaitTime;
        if (waitTime != -1) {
            wait = unit.toMillis(waitTime);
        }

        RFuture<Void> f = evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_VOID,
                // get the existing timeout for the thread to remove
                "local queue = redis.call('lrange', KEYS[1], 0, -1);" +
                        // find the location in the queue where the thread is
                        "local i = 1;" +
                        "while i <= #queue and queue[i] ~= ARGV[1] do " +
                        "i = i + 1;" +
                        "end;" +
                        // go to the next index which will exist after the current thread is removed
                        "i = i + 1;" +
                        // decrement the timeout for the rest of the queue after the thread being removed
                        "while i <= #queue do " +
                        "redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), queue[i]);" +
                        "i = i + 1;" +
                        "end;" +
                        // remove the thread from the queue and timeouts set
                        "redis.call('zrem', KEYS[2], ARGV[1]);" +
                        "redis.call('lrem', KEYS[1], 0, ARGV[1]);",
                Arrays.asList(threadsQueueName, timeoutSetName),
                getLockName(threadId), wait);
        return f.toCompletableFuture();
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        long wait = threadWaitTime;
        if (waitTime != -1) {
            wait = unit.toMillis(waitTime);
        }

        long currentTime = System.currentTimeMillis();
        if (command == RedisCommands.EVAL_NULL_BOOLEAN) {
            return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    "while true do " +
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                        "if firstThreadId2 == false then " +
                            "break;" +
                        "end;" +
                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                        "if timeout <= tonumber(ARGV[3]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            "redis.call('lpop', KEYS[2]);" +
                        "else " +
                            "break;" +
                        "end;" +
                    "end;" +

                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                        "redis.call('lpop', KEYS[2]);" +
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                        "for i = 1, #keys, 1 do " +
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i]);" +
                        "end;" +

                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +
                    "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +
                    "return 1;",
                    Arrays.asList(getRawName(), threadsQueueName, timeoutSetName),
                    unit.toMillis(leaseTime), getLockName(threadId), currentTime, wait);
        }

        if (command == RedisCommands.EVAL_LONG) {
            // KEYS[1]是锁名称, KEYS[2]是队列名称, KEYS[3]是zset名称
            // ARGS[1]是锁的存活时间, ARGS[2]是当前线程的标识, ARGS[3]是锁的等待时间, ARGS[4]是当前时间
            return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    // 1. 进入循环, 将zset中的score小于当前时间的所有线程, 都从zset和队列中移除
                    "while true do " +
                        // 从队列左侧取出第一个值, 也就是线程id
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                        "if firstThreadId2 == false then " +
                            // 如果队列是空的, 就直接跳出这个循环
                            "break;" +
                        "end;" +

                        // 如果队列中有值, 就从zset里取出队列第一个元素, 这个线程的score, 作为超时时间timeout
                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                        "if timeout <= tonumber(ARGV[4]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            // 如果超时时间小于当前时间, 就从队列和zset中移除这个线程
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            "redis.call('lpop', KEYS[2]);" +
                        "else " +
                            "break;" +
                        "end;" +
                    "end;" +

                    // check if the lock can be acquired now
                    // 2.
                    // 如果锁不存在并且队列不存在, 说明还没有初始化过这个锁, 就允许加锁
                    // 如果锁不存在, 但是队列存在, 并且队列左侧第一个元素是当前线程id, 说明当前线程持有锁, 也允许加锁
                    // 如果锁存在, 就不允许直接加锁
                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                    // 锁key不存在 and (阻塞队列key不存在 or 阻塞队列左侧第一个元素==当前线程id)

                        // remove this thread from the queue and timeout set
                        // 从队列和超时zset里移除当前线程的数据, 如果没有也不阻塞
                        "redis.call('lpop', KEYS[2]);" +
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                        "for i = 1, #keys, 1 do " +
                            // 遍历zset里所有元素, 重新计算等待时间
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i]);" +
                        "end;" +

                        // acquire the lock and set the TTL for the lease
                        // 加锁, 并设置过期时间, 就结束了
                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +

                    // check if the lock is already held, and this is a re-entry
                    // 3. 如果锁已经存在了, 并且是当前线程持有锁
                    "if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then " +
                        // 就进行重入, 锁重入次数+1, 过期时间重置
                        "redis.call('hincrby', KEYS[1], ARGV[2],1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +

                    // the lock cannot be acquired
                    // check if the thread is already in the queue
                    // 4. 如果加锁失败了, 并且也不是当前线程持有锁, 但是当前线程已经在阻塞队列里排队了
                    // 获取zset里当前线程的score, 也就是
                    "local timeout = redis.call('zscore', KEYS[3], ARGV[2]);" +
                    "if timeout ~= false then " +
                        // the real timeout is the timeout of the prior thread
                        // in the queue, but this is approximately correct, and
                        // avoids having to traverse the queue
                        "return timeout - tonumber(ARGV[3]) - tonumber(ARGV[4]);" +
                    "end;" +

                    // add the thread to the queue at the end, and set its timeout in the timeout set to the timeout of
                    // the prior thread in the queue (or the timeout of the lock if the queue is empty) plus the
                    // threadWaitTime
                    // 5. 如果加锁失败了, 当前线程也没有持有锁, zset也没有这个线程的score,
                    // 说明是一个新线程来竞争锁, 或者是上面while循环里移除掉了, 就将它再次加入队列和zset里
                    // 拿到队列从左往右数的最后一个元素, 标记为lastThreadId
                    "local lastThreadId = redis.call('lindex', KEYS[2], -1);" +
                    "local ttl;" +
                    "if lastThreadId ~= false and lastThreadId ~= ARGV[2] then " +
                        // 如果最后一个线程就是当前线程, 就设置zset里这个线程的score为
                        "ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4]);" +
                    "else " +
                        // 否则就拿到锁key的剩余存活时间
                        "ttl = redis.call('pttl', KEYS[1]);" +
                    "end;" +
                    "local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4]);" +
                    // 将当前线程加入zset和队列右侧
                    "if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then " +
                        "redis.call('rpush', KEYS[2], ARGV[2]);" +
                    "end;" +
                    "return ttl;",
                    Arrays.asList(getRawName(), threadsQueueName, timeoutSetName),
                    unit.toMillis(leaseTime), getLockName(threadId), wait, currentTime);
        }

        throw new IllegalArgumentException();
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // KEYS[1]是锁名称, KEYS[2]是队列名称, KEYS[3]是zset名称, KEYS[4]是channel名称
                // ARGS[1]是解锁的消息, ARGS[3]是当前线程的标识, ARGS[4]是当前时间
                // remove stale threads
                // 1. 进入循环, 将zset中的score小于当前时间的所有线程, 都从zset和队列中移除
                "while true do "
                // 从队列左侧取出第一个值, 也就是线程id
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    // 如果队列是空的, 就直接跳出这个循环
                    + "break;"
                + "end; "
                // 如果队列中有值, 就从zset里取出队列第一个元素, 这个线程的score, 作为超时时间timeout
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[4]) then "
                    // 如果超时时间小于当前时间, 就从队列和zset中移除这个线程
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"

                // 2. 如果锁不存在, 可能是自动过期失效了
              + "if (redis.call('exists', KEYS[1]) == 0) then " +
                    // 发布事件, 通知下一个线程锁释放了, 让它来竞争锁
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " +
                    "return 1; " +
                "end;" +
                // 3. 如果锁存在, 但是不是当前线程持有的锁, 也就不能释放锁, 直接返回
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                    "return nil;" +
                "end; " +
                // 4. 如果锁存在, 并且是当前线程持有的锁, 就将锁重入次数-1
                "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                "if (counter > 0) then " +
                    // 如果锁重入次数>0, 说明还持有锁, 顺便对锁的过期时间重设一遍
                    "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                    "return 0; " +
                "end; " +

                // 5. 如果锁存在, 并且是当前线程持有的锁, 并且释放锁后, 锁重入次数为0, 说明完全释放掉这个锁了, 就删除掉这个锁key
                "redis.call('del', KEYS[1]); " +
                // 发布事件, 通知下一个线程锁释放了, 让它来竞争锁
                "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                "if nextThreadId ~= false then " +
                    "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                "end; " +
                "return 1; ",
                Arrays.asList(getRawName(), threadsQueueName, timeoutSetName, getChannelName()),
                LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId), System.currentTimeMillis());
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return deleteAsync(getRawName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        List<Object> keys = Arrays.asList(getRawName(), threadsQueueName, timeoutSetName);
        return super.sizeInMemoryAsync(keys);
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit, String param, String... keys) {
        return super.expireAsync(timeToLive, timeUnit, param, getRawName(), threadsQueueName, timeoutSetName);
    }

    @Override
    protected RFuture<Boolean> expireAtAsync(long timestamp, String param, String... keys) {
        return super.expireAtAsync(timestamp, param, getRawName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return clearExpireAsync(getRawName(), threadsQueueName, timeoutSetName);
    }

    
    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[2]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                + 
                
                "if (redis.call('del', KEYS[1]) == 1) then " + 
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " + 
                    "return 1; " + 
                "end; " + 
                "return 0;",
                Arrays.asList(getRawName(), threadsQueueName, timeoutSetName, getChannelName()),
                LockPubSub.UNLOCK_MESSAGE, System.currentTimeMillis());
    }

}