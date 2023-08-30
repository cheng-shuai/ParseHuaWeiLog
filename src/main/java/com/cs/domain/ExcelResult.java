package com.cs.domain;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Data;

/**
 * @author admin
 * @version 1.0
 * @description
 * @date 2023/8/28 19:38
 */
@Data
public class ExcelResult {
    @ExcelProperty("出错原因")
    private String reason;
    @ExcelProperty("服务名称")
    private String serverName;
    @ExcelProperty("日志内容")
    private String log;
}
