package com.msm.common.utiles;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by wzl on 5/27/21.
 * @author wzl
 */

public class ThreadPool {
    static ThreadPool mSelf;
    static ThreadFactory mNamedThreadFactory;
    static ExecutorService mPool;

    private ThreadPool(){
    }

    public static synchronized ThreadPool getInstance(){
        if(mSelf == null){
            mSelf = new ThreadPool();
            mNamedThreadFactory = Executors.defaultThreadFactory();
            mPool = new ThreadPoolExecutor(200, 1000,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(1024), mNamedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        }

        return mSelf;
    }

    public void execute(Runnable cmd){
        mPool.execute(cmd);
    }
}
