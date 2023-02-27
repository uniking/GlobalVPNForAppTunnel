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
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

public class TCPInput implements Runnable
{
    private static final String TAG = TCPInput.class.getSimpleName();

    private ConcurrentLinkedQueue<ByteBuffer> outputQueue;
    private Selector selector;
    private ReentrantLock tcpSelectorLock;

    public TCPInput(ConcurrentLinkedQueue<ByteBuffer> outputQueue, Selector selector,ReentrantLock tcpSelectorLock)
    {
        this.outputQueue = outputQueue;
        this.selector = selector;
        this.tcpSelectorLock=tcpSelectorLock;
    }

    @Override
    public void run()
    {
        try
        {
            LogUtils.i(TAG, "Started");
            while (!Thread.interrupted())
            {
                tcpSelectorLock.lock();
                tcpSelectorLock.unlock();

                //int readyChannels = selector.select();
                //change by wzl
                int readyChannels = selector.select(1000);
                //end

                if (readyChannels == 0) {
                    //add by wzl
                    tcpSelectorLock.lock();
                    Set<SelectionKey> keys = selector.keys();
                    Iterator<SelectionKey> keyIterator = keys.iterator();
                    while (keyIterator.hasNext() && !Thread.interrupted())
                    {
                        SelectionKey key = keyIterator.next();
                        TCB tcb = (TCB)key.attachment();
                        try {
                            if(tcb.channel.finishConnect()){
                                tcb.channel.socket().sendUrgentData(0xff);
                            }
                        }catch (Exception e){
                            //socket已经断开
                            LogUtils.i(TAG, tcb.ipAndPort+" has been closed.");
                            synchronized (tcb){
                                ByteBuffer receiveBuffer = ByteBufferPool.acquire();
                                sendRST(tcb, 0, receiveBuffer);
                                key.cancel();
                                //sendFIN(tcb, receiveBuffer);
                            }

                            break;
                        }
                    }
                    tcpSelectorLock.unlock();
                    //end add


                    Thread.sleep(11);
                    continue;
                }
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = keys.iterator();

                while (keyIterator.hasNext() && !Thread.interrupted())
                {
                    SelectionKey key = keyIterator.next();
                    if (key.isValid())
                    {
                        if (key.isConnectable())
                            processConnect(key, keyIterator);
                        else if (key.isReadable())
                            processInput(key, keyIterator);
                    }else{
                        LogUtils.i(TAG, "key is not valid");
                    }
                }
            }
        }
        catch (InterruptedException e)
        {
            LogUtils.i(TAG, "Stopping");
        }
        catch (IOException e)
        {
            LogUtils.w(TAG, e.toString(), e);
        }
    }

    private void processConnect(SelectionKey key, Iterator<SelectionKey> keyIterator)
    {
        TCB tcb = (TCB) key.attachment();
        Packet referencePacket = tcb.referencePacket;

        try
        {
            if (tcb.channel.finishConnect())
            {
                keyIterator.remove();
                tcb.status = TCB.TCBStatus.SYN_RECEIVED;

                // TODO: Set MSS for receiving larger packets from the device
                ByteBuffer responseBuffer = ByteBufferPool.acquire();
                referencePacket.updateTCPBuffer(responseBuffer, (byte) (Packet.TCPHeader.SYN | Packet.TCPHeader.ACK),
                        tcb.mySequenceNum, tcb.myAcknowledgementNum, 0);
                outputQueue.offer(responseBuffer);

                tcb.mySequenceNum++; // SYN counts as a byte
                key.interestOps(SelectionKey.OP_READ);
            }
        }
        catch (IOException e)
        {
            LogUtils.e(TAG, "Connection error: " + tcb.ipAndPort, e);
            ByteBuffer responseBuffer = ByteBufferPool.acquire();
//            referencePacket.updateTCPBuffer(responseBuffer, (byte) Packet.TCPHeader.RST, 0, tcb.myAcknowledgementNum, 0);
//            outputQueue.offer(responseBuffer);
//            TCB.closeTCB(tcb);
            sendRST(tcb, 0, responseBuffer);
        }
    }

    private void processInput(SelectionKey key, Iterator<SelectionKey> keyIterator)
    {
        keyIterator.remove();
        ByteBuffer receiveBuffer = ByteBufferPool.acquire();
        // Leave space for the header

        TCB tcb = (TCB) key.attachment();
        synchronized (tcb)
        {
            Packet referencePacket = tcb.referencePacket;
            receiveBuffer.position(referencePacket.IP_TRAN_SIZE);
            //inputChannel由TCPOutput创建, 直接连接的真实服务器, 从真实服务器读取数据后放入队列, 待VPNRunnable写入接口
            SocketChannel inputChannel = (SocketChannel) key.channel();
            int readBytes;
            try
            {
                //到这儿receiveBuffer的position指向了TCP的有效数据的位置
                //这里读到的是服务端发过来的TCP内容, 要想写入接口, 还要调用updateTCPBuffer更新receiveBuffer
                readBytes = inputChannel.read(receiveBuffer);
            }
            catch (IOException e)
            {
                //change by wzl
                LogUtils.e(TAG, "Network read error: " + tcb.ipAndPort, e);
//                referencePacket.updateTCPBuffer(receiveBuffer, (byte) Packet.TCPHeader.RST, 0, 0, 0);
//
//                Packet re = null;
//                try {
//                    receiveBuffer.position(0);
//                    re = new Packet(receiveBuffer);
//                    LogUtils.e(TAG, "send " + re.ipHeader.destinationAddress.getHostAddress()+":"+re.tcpHeader.destinationPort + " RST");
//                    receiveBuffer.position(referencePacket.IP_TRAN_SIZE);
//                } catch (UnknownHostException ex) {
//                    LogUtils.e(TAG, "Network read error: " + tcb.ipAndPort, e);
//                }
//
//                outputQueue.offer(receiveBuffer);
//
//                TCB.closeTCB(tcb);
                sendRST(tcb, 0, receiveBuffer);
                return;
            }

            if (readBytes == -1)
            {
                // End of stream, stop waiting until we push more data
                key.interestOps(0);
                tcb.waitingForNetworkData = false;

                if (tcb.status != TCB.TCBStatus.CLOSE_WAIT)
                {
                    ByteBufferPool.release(receiveBuffer);
                    return;
                }

                //change by wzl
//                tcb.status = TCB.TCBStatus.LAST_ACK;
//                referencePacket.updateTCPBuffer(receiveBuffer, (byte) Packet.TCPHeader.FIN, tcb.mySequenceNum, tcb.myAcknowledgementNum, 0);
//                tcb.mySequenceNum++; // FIN counts as a byte
                sendFIN(tcb, receiveBuffer);
                return;
                //end change
            }
            else
            {
                // XXX: We should ideally be splitting segments by MTU/MSS, but this seems to work without
                referencePacket.updateTCPBuffer(receiveBuffer, (byte) (Packet.TCPHeader.PSH | Packet.TCPHeader.ACK),
                        tcb.mySequenceNum, tcb.myAcknowledgementNum, readBytes);
                tcb.mySequenceNum += readBytes; // Next sequence number
                receiveBuffer.position(referencePacket.IP_TRAN_SIZE + readBytes);
            }
        }

        //通过inputChannel.read从服务器读取了数据, 扔到接口里给客户端
        outputQueue.offer(receiveBuffer);
    }



    /*
add by wzl
调用close, 客户端还可能发送
 */
    private void sendFIN(TCB tcb, ByteBuffer buffer){
        tcb.status = TCB.TCBStatus.LAST_ACK;
        tcb.referencePacket.updateTCPBuffer(buffer, (byte) (Packet.TCPHeader.FIN | Packet.TCPHeader.ACK), tcb.mySequenceNum, tcb.myAcknowledgementNum, 0);
        tcb.mySequenceNum++; // FIN counts as a byte

        try {
            Packet re = null;
            buffer.position(0);
            re = new Packet(buffer);
            LogUtils.e(TAG, "sendFIN: " + re.ipHeader.destinationAddress.getHostAddress()+":"+re.tcpHeader.destinationPort);
            buffer.position(tcb.referencePacket.IP_TRAN_SIZE);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        outputQueue.offer(buffer);
    }

    /*
    add by wzl
    强制关闭连接, 清理tcb
     */
    private void sendRST(TCB tcb, int prevPayloadSize, ByteBuffer buffer)
    {
        tcb.referencePacket.updateTCPBuffer(buffer, (byte) (Packet.TCPHeader.RST | Packet.TCPHeader.ACK), tcb.mySequenceNum, tcb.myAcknowledgementNum+prevPayloadSize, 0);

        try {
            Packet re = null;
            buffer.position(0);
            re = new Packet(buffer);
            LogUtils.e(TAG, "sendRST: " + re.ipHeader.destinationAddress.getHostAddress()+":"+re.tcpHeader.destinationPort);
            buffer.position(tcb.referencePacket.IP_TRAN_SIZE);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        outputQueue.offer(buffer);
        TCB.closeTCB(tcb);
    }
}
