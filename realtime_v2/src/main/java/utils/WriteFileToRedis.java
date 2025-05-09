package utils;

import redis.clients.jedis.Jedis;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @Package com.bg.sensitive_words.untis.WriteFileToRedis
 * @Author zeyang_jia
 * @Date 2025/5/8 21:27
 * @description: 将文件数据写入Redis（已添加认证，密码作为字符串处理）
 */
public class WriteFileToRedis {
    public static void main(String[] args) {
        String file_path = "D:\\idea_work\\Stream_dev\\realtime_v2\\src\\main\\resources\\word.txt";
        String redis_key = "sensitive_words";
        String redisHost = "cdh01";
        int redisPort = 6379;

        // 注意：密码需作为字符串处理，即使它是纯数字
        String redisPassword = "123456"; // 替换为实际密码（字符串形式）

        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            // 添加 Redis 认证
            if (redisPassword != null && !redisPassword.isEmpty()) {
                jedis.auth(redisPassword);
                System.out.println("Redis认证成功");
            } else {
                System.out.println("警告：Redis密码为空，可能导致认证失败");
            }

            // 验证连接是否成功
            String pingResponse = jedis.ping();
            if (!"PONG".equalsIgnoreCase(pingResponse)) {
                throw new IOException("Redis连接验证失败: " + pingResponse);
            }

            // 读取文件并写入 Redis
            try (BufferedReader reader = new BufferedReader(new FileReader(file_path))) {
                String line;
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    // 使用 SADD 命令将每行敏感词添加到集合
                    jedis.sadd(redis_key, line);
                    count++;
                }
                System.out.printf("成功将 %d 个敏感词写入 Redis (key: %s)%n", count, redis_key);
            }
        } catch (redis.clients.jedis.exceptions.JedisDataException e) {
            if (e.getMessage().contains("NOAUTH")) {
                System.err.println("认证失败：Redis需要密码，请检查密码是否正确");
            } else {
                System.err.println("Redis操作错误：" + e.getMessage());
            }
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("文件操作错误：" + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("发生未知错误：" + e.getMessage());
            e.printStackTrace();
        }
    }
}