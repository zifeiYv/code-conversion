/*
创建记录映射关系的表
ori_table    : 原始表的名称
ori_column   : 原始表的字段
tar_table    : 编码表的名称
tar_column   : 编码表的字段
value_column : 编码表的存储编码值的字段
explain_col  : 编码表对编码值进行说明的字段
*/
create table mapping_detail (
    id              integer primary key autoincrement not null,
    ori_table       text,
    ori_column      text,
    tar_table       text,
    tar_column      text,
    value_column    text,
    explain_col     text
);

/*
存储编码表的基本信息
tab_name   : 编码表的名称
code_col   : 记录编码的字段
value_col  : 记录编码值的字段
explain_col: 记录编码值对应含义的字段
table_type : 编码表的类型（1,表示为P_CODE类的编码表；2,表示为O_ORG类的编码表）
*/
create table code_table (
    id          integer primary key autoincrement not null,
    tab_name    text,
    code_col    text,
    value_col   text,
    explain_col text,
    table_type  integer
);
