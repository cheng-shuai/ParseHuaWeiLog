package com.cs;

import com.alibaba.excel.EasyExcel;
import com.cs.domain.ExcelResult;
import com.huaweicloud.sdk.core.auth.BasicCredentials;
import com.huaweicloud.sdk.core.auth.ICredential;
import com.huaweicloud.sdk.lts.v2.LtsClient;
import com.huaweicloud.sdk.lts.v2.model.ListLogsRequest;
import com.huaweicloud.sdk.lts.v2.model.ListLogsResponse;
import com.huaweicloud.sdk.lts.v2.model.LogContents;
import com.huaweicloud.sdk.lts.v2.model.QueryLtsLogParams;
import com.huaweicloud.sdk.lts.v2.region.LtsRegion;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class ParseHuaWeiLog {
    //文件名称的数量
    private int fileNum = 1;

    /**
     * 华为云日志解析
     *
     * @param executor  线程池
     * @param filePath  文件路径
     * @param LogStream 日志流id
     * @param LogGroup  日志组id
     */
    public void ParseHuaWeiOneDayLog(ThreadPoolExecutor executor, String filePath, String LogStream, String LogGroup) {
        //获取日志的开始时间
        long endTime = System.currentTimeMillis();
        //获取日志的结束时间,默认为获取一天之内的全部日志
        long startTime = endTime - 24 * 60 * 60 * 1000;
        String ak = "your ak";
        String sk = "your sk";
        ICredential auth = new BasicCredentials()
                .withAk(ak)
                .withSk(sk);
        LtsClient client = LtsClient.newBuilder()
                .withCredential(auth)
                .withRegion(LtsRegion.valueOf("cn-north-4"))
                .build();
        ListLogsRequest request = new ListLogsRequest();
        request.withLogGroupId(LogGroup);
        request.withLogStreamId(LogStream);
        QueryLtsLogParams body = new QueryLtsLogParams();
        body.withLimit(5000);
        body.withIsCount(true);
        body.withIsDesc(true);
        body.withEndTime(Long.toString(endTime));
        body.withStartTime(Long.toString(startTime));
        request.withBody(body);
        //每次获取单次最大条数 5000
        ListLogsResponse response = client.listLogs(request);
        AtomicLong count = new AtomicLong(1L);
        Set<String> set = new HashSet<>();
        //下次查询时存在日志条数才会进入此循环
        while (response.getCount() > 0) {
            //设置当前线程名称
            Thread.currentThread().setName(filePath);
            set.addAll(ParseLog(response.getLogs()));
            //定义每次发送请求的开始 lineNum
            String lineNum = response.getLogs().get(response.getCount() - 1).getLineNum();
            //添加上一次结束的lineNum来获取下次请求开始的lineNum
            body.withLineNum(lineNum);
            request.withBody(body);
            response = client.listLogs(request);
            set.addAll(ParseLog(response.getLogs()));
            //每查过100000条写一次并且清空list
            if (set.size() > 400000) {
                CompletableFuture.runAsync(() -> {
                    appendListToFile(set, filePath, count.getAndIncrement());
                    set.clear();
                }, executor).whenComplete((r, e) -> {
                    if (Objects.nonNull(e)) {
                        log.error("写入文件失败", e);
                    }
                });
            }
        }
        //未超过可能剩余的部分
        if (!set.isEmpty()) {
            appendListToFile(set, filePath, count.getAndIncrement());
        }
        log.info(filePath + "写入完成");
    }

    /**
     * 解析日志
     *
     * @param logs 日志内容
     * @return 解析后的日志
     */
    public List<String> ParseLog(List<LogContents> logs) {
        if (Objects.isNull(logs)) {
            return new ArrayList<>();
        }
        String s = logs.get(0).getContent().replaceAll("\n", "");
        if (!s.matches("^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\] \\[([^\\]]+)\\] \\[([^\\]]+)\\] \\[TID: ([^\\]]+)\\] \\[([A-Z]+)\\] \\[([^\\]]+)\\] \\[([^\\]]+)\\] \\[([^\\]]+)\\](.+)")) {
            return logs.stream().map(LogContents::getContent).collect(Collectors.toList());
        }
        return logs.stream().
                filter(log -> Objects.nonNull(log.getContent()) && log.getContent().length() > 26)
                .map(log -> {
                    String content = log.getContent().replaceAll("\n", "");
                    return content.substring(26);
                }).distinct().filter(log -> {
                    log = log.trim();
                    int first = log.indexOf(":");
                    int secondColonIndex = log.indexOf(":", first + 1);
                    if (secondColonIndex != -1) {
                        String sub = log.substring(secondColonIndex + 1).trim();
                        return !sub.matches("^[a-zA-Z0-9,: _\\u4e00-\\u9fa5\\\\p{L}]{1,100}$");
                    }
                    return false;
                }).collect(Collectors.toList());
    }

    /**
     * 将set写入文件
     *
     * @param set      日志内容
     * @param filePath 文件路径
     */
    public void appendListToFile(Set<String> set, String filePath, long count) {
        try {
            System.out.printf("开始第" + count + "次写文件:" + (filePath) + "%n", fileNum);
            Path parent = Paths.get(filePath).getParent();
            if (Files.notExists(parent)) {
                Files.createDirectories(parent);
            }
            Path path = Paths.get(String.format(filePath, fileNum));
            if (Files.notExists(path)) {
                Files.createFile(path);
            }
            //如果当前文件超过2G，就写入下一个文件
            if (Files.size(path) > 2 * 1024 * 1024 * 1024L) {
                log.info(path + " 文件超过2G,开始写入下一个文件");
                fileNum++;
            }
            //写入不同的文件中,第一次写入则不追加,否则追加
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(String.format(filePath, fileNum), !(count == 1)))) {
                for (String element : set) {
                    writer.write(element.trim() + "\n");
                }
                writer.flush();
            }
        } catch (IOException e) {
            System.err.println(e);
        }
    }


    public void appendListToExcel(Set<String> set, String excelFolderPath) {
        Pattern pattern0 = Pattern.compile("^\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\] \\[([^\\]]+)\\] \\[([^\\]]+)\\] \\[TID: ([^\\]]+)\\] \\[([A-Z]+)\\] \\[([^\\]]+)\\] \\[([^\\]]+)\\] \\[([^\\]]+)\\](.+)");
        //Pattern pattern = Pattern.compile("^[a-zA-Z0-9,: _\\u4e00-\\u9fa5\\\\p{L}]{1,100}$");
        Path path = Paths.get(excelFolderPath);
        Path parent = path.getParent();
        try {
            if (Files.notExists(parent)) {
                Files.createDirectories(parent);
            }
            if (Files.notExists(path)) {

                Files.createFile(path);
            }
        } catch (Exception e) {
            log.error("出现错误:", e);
        }
        ArrayList<ExcelResult> excelResults = new ArrayList<>();
        for (String s : set) {
            ExcelResult excelResult = new ExcelResult();
            String sub = s.substring(s.indexOf(":", s.indexOf(":") + 1) + 1);
            //Matcher matcher = pattern.matcher(sub);
            StringBuilder reason = new StringBuilder("不符合原因:");
            if (!pattern0.matcher(s).matches()) {
                reason.append("不符合整体的日志格式");
            } else {
                if (sub.length() > 100) {
                    reason.append("字符数大于100;");
                }
                String disallowedCharacters = sub.replaceAll("^[a-zA-Z0-9,: _\\u4e00-\\u9fa5\\\\p{L}]{1,100}$", "");
                if (!disallowedCharacters.isEmpty()) {
                    reason.append("包含不允许的字符: ").append(disallowedCharacters);
                }
            }
            excelResult.setLog(s);
            excelResult.setReason(reason.toString());
            excelResult.setServerName(path.getName(2).toString());
            excelResults.add(excelResult);
        }

        try {
            //
            EasyExcel.write(Files.newOutputStream(path), ExcelResult.class).sheet("Sheet" + fileNum).doWrite(excelResults);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("写入" + excelFolderPath + "的Sheet" + fileNum);
        fileNum++;
    }

    /**
     * 解析csv文件
     *
     * @return 解析后需要的结果
     */
    public static List<String[]> ParseCSVFile(String csvPath) {
        try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
            return reader.readAll();
        } catch (IOException | CsvException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) {
        String parent = "D:\\Data\\LogOptimization1\\";
        ThreadPoolExecutor writeFile = new ThreadPoolExecutor(5, 5, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10));
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10));
        //解析csv文件
        //解析华为云日志
        List<CompletableFuture<Void>> list = new ArrayList<>();
        //Thread thread = null;
        for (String[] strings : ParseCSVFile(parent + "newLog2.csv")) {
            //thread = new Thread(() -> new ParseHuaWeiLog().ParseHuaWeiOneDayLog(executor, parent + strings[2] + "\\" + strings[2] + "-%d.txt", strings[1], strings[0]));
            //thread.start();
            try {
                CompletableFuture<Void> future = CompletableFuture
                        .runAsync(() -> new ParseHuaWeiLog().ParseHuaWeiOneDayLog(writeFile, parent + strings[2] + "\\" + strings[2] + ".xlsx", strings[1], strings[0]), executor);
                list.add(future);
            } catch (Exception e) {
                log.error("出现错误:", e);
            }
        }
        //if (thread != null)
        //thread.join();
        if (!list.isEmpty())
            CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).join();
        writeFile.shutdown();
        executor.shutdown();
        log.info("程序完成");
    }
}
