package com.cs;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * @author cs
 * @version 1.0
 * @description 解析日志不合格原因
 * @date 2023/8/30 13:43
 */
public class ParseLogReason {
    public static void main1(String[] args) throws IOException {
        Path path = Paths.get("D:\\Data\\LogOptimization\\order-pro-server");
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String fileName = file.getName(3).toString();
                String outputFilePath = path + "\\" + fileName.substring(0, fileName.lastIndexOf("_")) + ".txt";
                try (BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
                     BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
                    String cc = "不符合原因:";
                    String line;
                    while ((line = reader.readLine()) != null) {
                        int index = line.indexOf(cc);
                        if (index != -1) {
                            writer.write(line.substring(0, index));
                        } else {
                            writer.write(line);
                        }
                        writer.newLine();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Files.delete(file);
                return super.visitFile(file, attrs);
            }
        });
    }

    public static void main(String[] args) throws IOException {
        Files.walkFileTree(Paths.get("D:\\Data\\LogOptimization\\sys-server"), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String outputFilePath = file.getParent() + "\\" + file.getFileName().toString().substring(0, file.getFileName().toString().lastIndexOf(".")) + "_output.txt";
                //Pattern pattern = Pattern.compile("^[a-zA-Z0-9,:\"'\\u4e00-\\u9fa5\\[\\]\\s]*$");
                try (BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
                     FileWriter writer = new FileWriter(outputFilePath)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        //String sub = line.substring(line.indexOf(":", line.indexOf(":") + 1) + 1);

                        String sub = line.substring(line.indexOf(":") + 1);
                        /*sub = sub.substring(sub.indexOf(":") + 1);
                        sub = sub.substring(sub.indexOf(":") + 1);
                        sub = sub.substring(sub.indexOf(":") + 1);
                        sub = sub.substring(sub.indexOf(":") + 1);*/
                        //Matcher matcher = pattern.matcher(sub);
                        //if (!matcher.matches()) {
                        StringBuilder reason = new StringBuilder("不符合原因:");
                        if (sub.length() > 300) {
                            reason.append("字符数大于300;");
                            writer.write(line + "   " + reason + "\n");
                        }
                        /*String disallowedCharacters = sub.replaceAll("[a-zA-Z0-9,:\"'\\u4e00-\\u9fa5\\[\\]\\s]*", "");
                        if (!disallowedCharacters.isEmpty()) {
                            reason.append("包含不允许的字符: ").append(disallowedCharacters);
                        }*/

                        //}
                    }
                    System.out.println("验证已完成，不符合的原因已写入输出文件。");
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
                return super.visitFile(file, attrs);
            }
        });

    }
}
