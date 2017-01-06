package com.pujjr.antifraud.test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
 
/**
 * @author qifuguang
 * @date 15-2-4 下午1:21
 */
public class SelectorTimeClient implements Runnable {
    private static final String TIME_ORDER = "Query Time";
    private SocketChannel channel;
    private Selector selector;
    private volatile boolean stop = false;
    private Integer index;
 
    /**
     * 创建Selector, SocketChannel.
     *
     * @param index 客户端编号.
     * @throws Exception
     */
    public SelectorTimeClient(Integer index) throws Exception {
        selector = Selector.open();
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        this.index = index;
    }
 
    /**
     * 轮询监听selector刚兴趣的事件.
     */
    @Override
    public void run() {
        try {
            System.out.println("第" + index + "个客户端启动!");
            // 先尝试异步连接服务器, 如果连接成功,则只需要把channel注册到selector的READ事件,
            // 读取服务器返回的结果. 如果不成功（客户端已经向服务器发送了sync包,但是服务器没有返回ack包, 物理链路还没建立成功）
            // 则把该channel注册到selector的CONNECT事件, 等待服务器返回的ack包.
            if (channel.connect(new InetSocketAddress(8080))) {
                channel.register(selector, SelectionKey.OP_READ);
                doWrite(channel, TIME_ORDER);
            } else {
                channel.register(selector, SelectionKey.OP_CONNECT);
            }
            while (!stop) {
                selector.select(1000);
                
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    SocketChannel sc = (SocketChannel) key.channel();
                    iterator.remove();
                    if (key.isValid()) {
                        if (key.isReadable()) {  // 监听到可读事件, 读取服务器返回的处理结果.
                            ByteBuffer buff = ByteBuffer.allocate(1024);
                            int size = sc.read(buff);
                            if (size > 0) {
                                byte[] b = new byte[size];
                                buff.flip();
                                buff.get(b);
                                System.out.println("第" + index + "个客户端获取服务器返回时间：" + new String(b));
                                stop = true;
                            } else if (size < 0) {
                                sc.close();
                                key.cancel();
                            } else {
                                ;
                            }
                        }
                        if (key.isConnectable()) {  //监听到服务器返回了ack包, 准备完成连接的建立
                            if (sc.finishConnect()) {  // 调用此方法完成物理链路连接的建立
                                sc.register(selector, SelectionKey.OP_READ); // 建立连接之后注册监听READ事件
                                doWrite(sc, TIME_ORDER);
                            } else {
                                System.exit(1);  //否则,程序退出
                            }
                        }
                    }
                }
            }
            if (selector != null) {
                selector.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 
    /**
     * 向指定的channel发送指定的消息.
     *
     * @param channel 向哪一个channel发送消息
     * @param content 需要发送的消息
     * @throws Exception
     */
    private void doWrite(SocketChannel channel, String content) throws Exception {
        ByteBuffer buff = ByteBuffer.allocate(1024);
        buff.put(content.getBytes("UTF-8"));
        buff.flip();
        channel.write(buff);
        if (!buff.hasRemaining()) {
            System.out.println("第" + index + "个客户端成功发送请求到服务器：" + content);
        }
    }
}
