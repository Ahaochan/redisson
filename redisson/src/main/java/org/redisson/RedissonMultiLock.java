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

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RLockAsync;
import org.redisson.client.RedisResponseTimeoutException;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

/**
 * Groups multiple independent locks and manages them as one lock.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonMultiLock implements RLock {

    class LockState {
        
        private final long newLeaseTime;
        private final long lockWaitTime;
        private final List<RLock> acquiredLocks;
        private final long waitTime;
        private final long threadId;
        private final long leaseTime;
        private final TimeUnit unit;

        private long remainTime;
        private long time = System.currentTimeMillis();
        private int failedLocksLimit;
        
        LockState(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
            this.waitTime = waitTime;
            this.leaseTime = leaseTime;
            this.unit = unit;
            this.threadId = threadId;
            
            if (leaseTime != -1) {
                if (waitTime == -1) {
                    newLeaseTime = unit.toMillis(leaseTime);
                } else {
                    newLeaseTime = unit.toMillis(waitTime)*2;
                }
            } else {
                newLeaseTime = -1;
            }

            remainTime = -1;
            if (waitTime != -1) {
                remainTime = unit.toMillis(waitTime);
            }
            lockWaitTime = calcLockWaitTime(remainTime);
            
            failedLocksLimit = failedLocksLimit();
            acquiredLocks = new ArrayList<>(locks.size());
        }
        
        void tryAcquireLockAsync(ListIterator<RLock> iterator, RPromise<Boolean> result) {
            if (!iterator.hasNext()) {
                checkLeaseTimeAsync(result);
                return;
            }

            RLock lock = iterator.next();
            RFuture<Boolean> lockAcquiredFuture;
            if (waitTime == -1 && leaseTime == -1) {
                lockAcquiredFuture = lock.tryLockAsync(threadId);
            } else {
                long awaitTime = Math.min(lockWaitTime, remainTime);
                lockAcquiredFuture = lock.tryLockAsync(awaitTime, newLeaseTime, TimeUnit.MILLISECONDS, threadId);
            }
            
            lockAcquiredFuture.whenComplete((res, e) -> {
                boolean lockAcquired = false;
                if (res != null) {
                    lockAcquired = res;
                }

                if (e instanceof RedisResponseTimeoutException) {
                    unlockInnerAsync(Arrays.asList(lock), threadId);
                }
                
                if (lockAcquired) {
                    acquiredLocks.add(lock);
                } else {
                    if (locks.size() - acquiredLocks.size() == failedLocksLimit()) {
                        checkLeaseTimeAsync(result);
                        return;
                    }

                    if (failedLocksLimit == 0) {
                        unlockInnerAsync(acquiredLocks, threadId).onComplete((r, ex) -> {
                            if (ex != null) {
                                result.tryFailure(ex);
                                return;
                            }
                            
                            if (waitTime == -1) {
                                result.trySuccess(false);
                                return;
                            }
                            
                            failedLocksLimit = failedLocksLimit();
                            acquiredLocks.clear();
                            // reset iterator
                            while (iterator.hasPrevious()) {
                                iterator.previous();
                            }
                            
                            checkRemainTimeAsync(iterator, result);
                        });
                        return;
                    } else {
                        failedLocksLimit--;
                    }
                }
                
                checkRemainTimeAsync(iterator, result);
            });
        }
        
        private void checkLeaseTimeAsync(RPromise<Boolean> result) {
            if (leaseTime != -1) {
                AtomicInteger counter = new AtomicInteger(acquiredLocks.size());
                for (RLock rLock : acquiredLocks) {
                    RFuture<Boolean> future = ((RedissonBaseLock) rLock).expireAsync(unit.toMillis(leaseTime), TimeUnit.MILLISECONDS);
                    future.onComplete((res, e) -> {
                        if (e != null) {
                            result.tryFailure(e);
                            return;
                        }
                        
                        if (counter.decrementAndGet() == 0) {
                            result.trySuccess(true);
                        }
                    });
                }
                return;
            }
            
            result.trySuccess(true);
        }
        
        private void checkRemainTimeAsync(ListIterator<RLock> iterator, RPromise<Boolean> result) {
            if (remainTime != -1) {
                remainTime += -(System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                if (remainTime <= 0) {
                    unlockInnerAsync(acquiredLocks, threadId).onComplete((res, e) -> {
                        if (e != null) {
                            result.tryFailure(e);
                            return;
                        }
                        
                        result.trySuccess(false);
                    });
                    return;
                }
            }
            
            tryAcquireLockAsync(iterator, result);
        }
        
    }
    
    final List<RLock> locks = new ArrayList<>();
    
    /**
     * Creates instance with multiple {@link RLock} objects.
     * Each RLock object could be created by own Redisson instance.
     *
     * @param locks - array of locks
     */
    public RedissonMultiLock(RLock... locks) {
        if (locks.length == 0) {
            throw new IllegalArgumentException("Lock objects are not defined");
        }
        this.locks.addAll(Arrays.asList(locks));
    }
    
    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lockInterruptibly(leaseTime, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        return lockAsync(leaseTime, unit, Thread.currentThread().getId());
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long threadId) {
        long baseWaitTime = locks.size() * 1500;
        long waitTime = -1;
        if (leaseTime == -1) {
            waitTime = baseWaitTime;
        } else {
            leaseTime = unit.toMillis(leaseTime);
            waitTime = leaseTime;
            if (waitTime <= 2000) {
                waitTime = 2000;
            } else if (waitTime <= baseWaitTime) {
                waitTime = ThreadLocalRandom.current().nextLong(waitTime/2, waitTime);
            } else {
                waitTime = ThreadLocalRandom.current().nextLong(baseWaitTime, waitTime);
            }
        }
        
        RPromise<Void> result = new RedissonPromise<Void>();
        tryLockAsync(threadId, leaseTime, TimeUnit.MILLISECONDS, waitTime, result);
        return result;
    }
    
    protected void tryLockAsync(long threadId, long leaseTime, TimeUnit unit, long waitTime, RPromise<Void> result) {
        tryLockAsync(waitTime, leaseTime, unit, threadId).onComplete((res, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }
            
            if (res) {
                result.trySuccess(null);
            } else {
                tryLockAsync(threadId, leaseTime, unit, waitTime, result);
            }
        });
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        lockInterruptibly(-1, null);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        long baseWaitTime = locks.size() * 1500;
        while (true) {
            // 计算出一个waitTime
            long waitTime;
            if (leaseTime == -1) {
                waitTime = baseWaitTime;
            } else {
                waitTime = unit.toMillis(leaseTime);
                if (waitTime <= baseWaitTime) {
                    waitTime = ThreadLocalRandom.current().nextLong(waitTime/2, waitTime);
                } else {
                    waitTime = ThreadLocalRandom.current().nextLong(baseWaitTime, waitTime);
                }
            }

            // 不停死循环, 阻塞, 直到获取到所有的锁才退出循环
            if (tryLock(waitTime, leaseTime, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
    }

    @Override
    public boolean tryLock() {
        try {
            return tryLock(-1, -1, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    protected void unlockInner(Collection<RLock> locks) {
        locks.stream()
                .map(RLockAsync::unlockAsync)
                .forEach(f -> {
                    try {
                        f.toCompletableFuture().join();
                    } catch (Exception e) {
                        // skip
                    }
                });
    }
    
    protected RFuture<Void> unlockInnerAsync(Collection<RLock> locks, long threadId) {
        if (locks.isEmpty()) {
            return RedissonPromise.newSucceededFuture(null);
        }
        
        RPromise<Void> result = new RedissonPromise<Void>();
        AtomicInteger counter = new AtomicInteger(locks.size());
        for (RLock lock : locks) {
            lock.unlockAsync(threadId).onComplete((res, e) -> {
                if (e != null) {
                    result.tryFailure(e);
                    return;
                }
                
                if (counter.decrementAndGet() == 0) {
                    result.trySuccess(null);
                }
            });
        }
        return result;
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }
    
    protected int failedLocksLimit() {
        return 0;
    }
    
    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
//        try {
//            return tryLockAsync(waitTime, leaseTime, unit).get();
//        } catch (ExecutionException e) {
//            throw new IllegalStateException(e);
//        }
        long newLeaseTime = -1;
        if (leaseTime != -1) {
            if (waitTime == -1) {
                newLeaseTime = unit.toMillis(leaseTime);
            } else {
                newLeaseTime = unit.toMillis(waitTime)*2;
            }
        }
        
        long time = System.currentTimeMillis();
        long remainTime = -1;
        if (waitTime != -1) {
            remainTime = unit.toMillis(waitTime);
        }
        // 每个锁的获取超时时间, 超过就获取锁失败
        long lockWaitTime = calcLockWaitTime(remainTime);

        // 加锁失败的数量, 允许多少个子锁加锁失败
        int failedLocksLimit = failedLocksLimit();
        // 遍历底层的每一个锁
        List<RLock> acquiredLocks = new ArrayList<>(locks.size());
        for (ListIterator<RLock> iterator = locks.listIterator(); iterator.hasNext();) {
            RLock lock = iterator.next();
            boolean lockAcquired;
            try {
                if (waitTime == -1 && leaseTime == -1) {
                    // 加锁
                    lockAcquired = lock.tryLock();
                } else {
                    // 加锁
                    long awaitTime = Math.min(lockWaitTime, remainTime);
                    lockAcquired = lock.tryLock(awaitTime, newLeaseTime, TimeUnit.MILLISECONDS);
                }
            } catch (RedisResponseTimeoutException e) {
                // 任意一个锁异常, 就解锁所有锁
                unlockInner(Arrays.asList(lock));
                lockAcquired = false;
            } catch (Exception e) {
                lockAcquired = false;
            }
            
            if (lockAcquired) {
                // 加锁成功, 将这个锁加入集合
                acquiredLocks.add(lock);
            } else {
                if (locks.size() - acquiredLocks.size() == failedLocksLimit()) {
                    // 这里刚好到最后一个锁加锁失败, 那也算加锁成功, 直接break出去
                    break;
                }

                // 如果failedLocksLimit为0, 意味着不允许再加锁失败了, 就解锁所有成功的锁
                if (failedLocksLimit == 0) {
                    // 任意一个锁加锁失败, 就把加成功的锁给解锁了
                    unlockInner(acquiredLocks);
                    if (waitTime == -1) {
                        return false;
                    }
                    failedLocksLimit = failedLocksLimit();
                    acquiredLocks.clear();
                    // reset iterator
                    while (iterator.hasPrevious()) {
                        iterator.previous();
                    }
                } else {
                    // 如果failedLocksLimit不为0, 说明还有失败次数可以容忍, 继续加下一个锁
                    failedLocksLimit--;
                }
            }
            
            if (remainTime != -1) {
                remainTime -= System.currentTimeMillis() - time;
                time = System.currentTimeMillis();
                if (remainTime <= 0) {
                    unlockInner(acquiredLocks);
                    return false;
                }
            }
        }

        if (leaseTime != -1) {
            acquiredLocks.stream()
                    .map(l -> (RedissonBaseLock) l)
                    .map(l -> l.expireAsync(unit.toMillis(leaseTime), TimeUnit.MILLISECONDS))
                    .forEach(f -> f.toCompletableFuture().join());
        }
        
        return true;
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        RPromise<Boolean> result = new RedissonPromise<Boolean>();
        LockState state = new LockState(waitTime, leaseTime, unit, threadId);
        state.tryAcquireLockAsync(locks.listIterator(), result);
        return result;
    }
    
    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        return tryLockAsync(waitTime, leaseTime, unit, Thread.currentThread().getId());
    }

    
    protected long calcLockWaitTime(long remainTime) {
        return remainTime;
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        return unlockInnerAsync(locks, threadId);
    }
    
    @Override
    public void unlock() {
        List<RFuture<Void>> futures = new ArrayList<>(locks.size());

        // 对所有锁进行异步解锁
        for (RLock lock : locks) {
            futures.add(lock.unlockAsync());
        }

        // 然后异步转同步join进来
        for (RFuture<Void> future : futures) {
            future.toCompletableFuture().join();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        return unlockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Void> lockAsync(long threadId) {
        return lockAsync(-1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return tryLockAsync(-1, -1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Integer> getHoldCountAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceUnlock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Long> remainTimeToLiveAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long remainTimeToLive() {
        throw new UnsupportedOperationException();
    }

}
