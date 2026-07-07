package com.iwhaleai.byai.framework.core.discovery;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class DiscoveryUtils {

    /**
     * 获取本机出口 IP 地址。
     * <p>
     * 通过尝试连接目标主机（不发送数据）来探测本地网卡 IP。
     * 如果 targetHost 是 localhost 或 127.0.0.1，则尝试连接公网地址探测，
     * 若仍失败或环境受限，尝试返回一个非回环地址。
     */
    public static String getLocalIp(String targetHost, int targetPort) {
        boolean isLoopback = "127.0.0.1".equals(targetHost) || "localhost".equals(targetHost) || "::1".equals(targetHost);
        
        String actualTargetHost = isLoopback ? "8.8.8.8" : targetHost;
        int actualTargetPort = isLoopback ? 80 : targetPort;

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName(actualTargetHost), actualTargetPort);
            return socket.getLocalAddress().getHostAddress();
        } catch (SocketException | UnknownHostException | java.io.UncheckedIOException e) {
            // 兜底：获取主机名对应的 IP
            // DatagramSocket.connect() 在部分 JDK 版本下（NIO 适配的
            // DatagramSocketAdaptor）会把底层 IOException 包装成
            // UncheckedIOException 而不是抛出受检的 SocketException，
            // 例如网络不可达（无出网权限的容器/沙箱环境）时；必须一并捕获，
            // 否则这条兜底路径永远不会被触发。
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException ex) {
                return "127.0.0.1";
            }
        }
    }

    public static String getLocalIp() {
        return getLocalIp("8.8.8.8", 80);
    }
}
