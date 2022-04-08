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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonReadLock extends RedissonLock implements RLock {

    public RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }
    
    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        // getLockName(threadId): 命令执行器UUID:线程Id
        // getRawName(): 锁名称
        // suffixName(): {锁名称}:命令执行器UUID:线程Id
        // result: {锁名称}:命令执行器UUID:线程Id:rwlock_timeout
        return suffixName(getRawName(), getLockName(threadId)) + ":rwlock_timeout";
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                                // KEYS[1]是锁名称, KEYS[2]是当前线程标识
                                // ARGV[1]是过期时间, ARGV[2]是当前线程标识, ARGV[2]是当前线程写锁标识

                                // 获取hash结构的锁key里的mode属性, 判断现在是读锁还是写锁
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                "if (mode == false) then " +
                                  // 如果锁不存在, 就设置成读锁, 锁重入次数+1,
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                  // 设置XXX为1
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +
                                  // 再设置过期时间为30000毫秒
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                                "end; " +
                                // 如果已经加了读锁, 可以重复加读锁
                                // 如果已经加了写锁, 并且是当前线程持有了写锁, 可以重复加读锁
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                                  // 锁重入次数+1
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " + 
                                  "local key = KEYS[2] .. ':' .. ind;" +
                                  "redis.call('set', key, 1); " +
                                  "redis.call('pexpire', key, ARGV[1]); " +
                                  "local remainTime = redis.call('pttl', KEYS[1]); " +
                                  "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                                  "return nil; " +
                                "end;" +
                                // 如果加了写锁, 但不是当前线程持有的写锁, 就阻塞获取锁, 返回锁的剩余过期时间
                                "return redis.call('pttl', KEYS[1]);",
                        Arrays.<Object>asList(getRawName(), getReadWriteTimeoutNamePrefix(threadId)),
                        unit.toMillis(leaseTime), getLockName(threadId), getWriteLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        // {锁名称}:命令执行器UUID:线程Id:rwlock_timeout
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        // {锁名称}
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // KEYS[1]是锁名称, KEYS[2]是channel名称, KEYS[3]和KEYS[4]在上面
                // ARGV[1]是解锁的消息, ARGV[2]是当前线程加锁标识

                // 获取hash结构的锁key里的mode属性, 判断现在是读锁还是写锁
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    // 如果锁不存在, 就发布解锁消息到channel中
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                // 判断当前线程是否加过锁, 没有就不用解锁
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                "if (lockExists == 0) then " +
                    "return nil;" +
                "end; " +

                // 如果当前线程加过读锁, 锁的次数就-1
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " + 
                "if (counter == 0) then " +
                    // 如果锁重入次数为0, 说明完全解锁了, 就把这个锁key删除了
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +
                
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    "local maxRemainTime = -3; " + 
                    "local keys = redis.call('hkeys', KEYS[1]); " +
                    // 遍历hash结构的锁key, 取rwlock_timeout的最大值
                    "for n, key in ipairs(keys) do " + 
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                        "if type(counter) == 'number' then " + 
                            "for i=counter, 1, -1 do " + 
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " + 
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" + 
                            "end; " +
                        "end; " +
                    "end; " +

                    // 将rwlock_timeout的最大值, 更新到当前锁key的过期时间
                    "if maxRemainTime > 0 then " +
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "return 0; " +
                    "end;" + 

                    // 如果已经没有rwlock_timeout了, 说明读写锁释放完毕
                    // 然后如果是写锁, 就直接返回了, 因为这里只是解读锁
                    "if mode == 'write' then " + 
                        "return 0;" + 
                    "end; " +
                "end; " +

                // 否则如果是写锁的话, 就删除这个锁, 然后发布解锁消息
                "redis.call('del', KEYS[1]); " +
                "redis.call('publish', KEYS[2], ARGV[1]); " +
                "return 1; ",
                Arrays.<Object>asList(getRawName(), getChannelName(), timeoutPrefix, keyPrefix),
                LockPubSub.UNLOCK_MESSAGE, getLockName(threadId));
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        // getLockName(threadId): 命令执行器UUID:线程Id
        // timeoutPrefix: {锁名称}:命令执行器UUID:线程Id:rwlock_timeout
        // 结果: {锁名称}
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }
    
    @Override
    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        // {锁名称}:命令执行器UUID:线程Id:rwlock_timeout
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        // {锁名称}
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);
        
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // KEYS[1]是锁名称, KEYS[2]是{锁名称}
                // ARGV[1]是过期时间, ARGV[2]是当前线程加锁标识
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
                "if (counter ~= false) then " +
                    // 当前线程的加锁次数不为0, 就说明有必要进行续期
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +

                    // 遍历所有锁key的所有rwlock_timeout属性, 为其续期
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        "local keys = redis.call('hkeys', KEYS[1]); " + 
                        "for n, key in ipairs(keys) do " + 
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                            "if type(counter) == 'number' then " +
                                // 等价于for(int i = count; i >= 1; i--)
                                "for i=counter, 1, -1 do " +
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " + 
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                    
                    "return 1; " +
                "end; " +
                "return 0;",
            Arrays.<Object>asList(getRawName(), keyPrefix),
            internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.<Object>asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "read".equals(res);
    }

}
