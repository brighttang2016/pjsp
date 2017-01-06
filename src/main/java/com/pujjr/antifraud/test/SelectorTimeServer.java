package com.pujjr.antifraud.test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
 
/**
 * @author qifuguang
 * @date 15-2-4 下午1:21
 */
public class SelectorTimeServer implements Runnable {
    private static final String TIME_ORDER = "Query Time";
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private volatile boolean stop = false;
 
    /**
     * 创建Selector, 创建ServerSocketChannel,并设置为非阻塞模式, 注册到selector.
     *
     * @throws Exception
     */
    public SelectorTimeServer() throws Exception {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.socket().bind(new InetSocketAddress(8080));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }
 
    /**
     * 轮询监听selector.
     */
    @Override
    public void run() {
        try {
            System.out.println("时间服务器启动！");
            while (!stop) {
                selector.select(1000);
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    handleKey(key);
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
     * 处理每一种selector感兴趣的事件.
     *
     * @param key 轮询监听得到的SelectionKey.
     */
    private void handleKey(SelectionKey key) {
        try {
            if (key.isValid()) {  // 如果连接成功
                if (key.isAcceptable()) {  // 监听到有新客户端连接
                    SocketChannel accept = ((ServerSocketChannel) key.channel()).accept(); // 建立与客户端的连接
                    accept.configureBlocking(false);  // 设置该连接为非阻塞模式
                    accept.register(selector, SelectionKey.OP_READ); // 将该连接注册到selector
                    System.out.println("发现有新客户端连接...");
                }
                if (key.isReadable()) {    // 监听到有客户端发送请求
                    SocketChannel channel = (SocketChannel) key.channel();
                    // 读取客户端发来的请求
                    ByteBuffer buff = ByteBuffer.allocate(1024);
                    int size = channel.read(buff);
                    if (size > 0) {
                        byte[] b = new byte[size];
                        buff.flip();
                        buff.get(b);
                        String order = new String(b, "UTF-8");
                        System.out.println("收到客户端命令：" + order);
                        String content = "";
                        if (order.equalsIgnoreCase(TIME_ORDER)) {
                            content = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                        } else {
                            content = "命令错误";
                        }
                        // 根据客户端发来的请求做出相应的动作,并将处理结果返回给客户端
                        doWrite(channel, content);
                    } else if (size < 0) {
                        channel.close();
                        key.cancel();
                    } else {
                        ;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 
    /**
     * 向指定的SocketChannel发送指定的消息。
     *
     * @param sc      需要向哪一个SocketChannel发送消息
     * @param content 需要发送的消息
     * @throws Exception
     */
    private void doWrite(SocketChannel sc, String content) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put(content.getBytes("UTF-8"));
        buffer.flip();
        sc.write(buffer);
        if (!buffer.hasRemaining()) {
            System.out.println("下发消息给客户端：" + content);
        }
    }
}
