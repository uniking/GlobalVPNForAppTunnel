/*
** Copyright 2015, Mohamed Naufal
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/

package com.github.xfalcon.vhosts.vservice;

import com.github.xfalcon.vhosts.util.LogUtils;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;

/**
 * Transmission Control Block
 */
public class TCB
{
    public String ipAndPort;

    public long mySequenceNum, theirSequenceNum;
    public long myAcknowledgementNum, theirAcknowledgementNum;
    public TCBStatus status;

    // TCP has more states, but we need only these
    public enum TCBStatus
    {
        SYN_SENT,//和服务端的connect没有完成,
        SYN_RECEIVED,//和服务端的connect已经完成
        ESTABLISHED,//如果SYN_RECEIVED完成, 开始监听read事件
        CLOSE_WAIT,//一会儿关闭, 等待网络数据
        LAST_ACK,//发生FIN
    }

    public Packet referencePacket;//tcb关联的ip包

    public SocketChannel channel;
    public boolean waitingForNetworkData;
    public SelectionKey selectionKey;

    public boolean TcpTunnelHead = false;

    private static final int MAX_CACHE_SIZE = 50; // XXX: Is this ideal?
    private static LRUCache<String, TCB> tcbCache =
            new LRUCache<>(MAX_CACHE_SIZE, new LRUCache.CleanupCallback<String, TCB>()
            {
                @Override
                public void cleanup(Map.Entry<String, TCB> eldest)
                {
                    eldest.getValue().closeChannel();
                }
            });

    public static TCB getTCB(String ipAndPort)
    {
        synchronized (tcbCache)
        {
            return tcbCache.get(ipAndPort);
        }
    }

    public static void putTCB(String ipAndPort, TCB tcb)
    {
        synchronized (tcbCache)
        {
            tcbCache.put(ipAndPort, tcb);
        }
    }

    public TCB(String ipAndPort, long mySequenceNum, long theirSequenceNum, long myAcknowledgementNum, long theirAcknowledgementNum,
               SocketChannel channel, Packet referencePacket)
    {
        this.ipAndPort = ipAndPort;

        this.mySequenceNum = mySequenceNum;
        this.theirSequenceNum = theirSequenceNum;
        this.myAcknowledgementNum = myAcknowledgementNum;
        this.theirAcknowledgementNum = theirAcknowledgementNum;

        this.channel = channel;
        this.referencePacket = referencePacket;
    }

    public static void closeTCB(TCB tcb)
    {
        LogUtils.i("TCP", "closeTCB " + tcb.ipAndPort);
        tcb.closeChannel();
        synchronized (tcbCache)
        {
            tcbCache.remove(tcb.ipAndPort);
        }
    }

    public static void closeAll()
    {
        synchronized (tcbCache)
        {
            Iterator<Map.Entry<String, TCB>> it = tcbCache.entrySet().iterator();
            while (it.hasNext())
            {
                it.next().getValue().closeChannel();
                it.remove();
            }
        }
    }

    private void closeChannel()
    {
        try
        {
            channel.close();
        }
        catch (IOException e)
        {
            // Ignore
        }
    }
}
