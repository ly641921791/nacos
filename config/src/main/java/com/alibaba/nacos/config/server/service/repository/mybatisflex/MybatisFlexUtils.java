package com.alibaba.nacos.config.server.service.repository.mybatisflex;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.model.Page;
import com.mybatisflex.core.query.QueryColumn;
import com.mybatisflex.core.query.QueryCondition;
import com.mybatisflex.core.query.QueryWrapper;
import com.mybatisflex.core.util.LambdaGetter;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MybatisFlexUtils {

    /**
     * concat =.
     *
     * @param queryChain queryChain
     * @param column     column
     * @param value      value
     * @param <T>        T
     */
    public static <T> void concatEq(QueryWrapper queryChain, LambdaGetter<T> column, String value) {
        // Oracle 中插入"" 变成 null，因此当 value 输入 "" 时，需要通过 is bull 去判断
        // "     " 不存在这种问题
        if (StringUtils.isEmpty(value)) {
            queryChain.isNull(column);
        } else {
            queryChain.eq(column, value);
        }
    }

    /**
     * concat in.
     *
     * @param queryChain queryChain
     * @param column     column
     * @param values     values
     */
    public static void concatIn(QueryWrapper queryChain, QueryColumn column, List<String> values) {
        // Oracle 中插入"" 变成 null，因此当 value 输入 "" 时， 通过 in 查不到， 需要拼接一个 is null
        QueryCondition queryCondition = column.in(values);
        if (values.contains("")) {
            queryCondition = queryCondition.or(column.isNull());
        }
        queryChain.and(queryCondition);
    }

    /**
     * concat not in.
     *
     * @param queryChain queryChain
     * @param column     column
     * @param values     values
     */
    public static void concatNotIn(QueryWrapper queryChain, QueryColumn column, List<String> values) {
        // Oracle 中插入"" 变成 null，因此当 value 输入 "" 时， 通过 in 查不到， 需要拼接一个 is null
        QueryCondition queryCondition = column.notIn(values);
        if (values.contains("")) {
            queryCondition = queryCondition.or(column.isNotNull());
        }
        queryChain.and(queryCondition);
    }

    /**
     * Mybatis Flex Page -> Nacos Page
     *
     * @param paginate Page
     * @param <T>      T
     * @return Page
     */
    public static <T> Page<T> convertPage(com.mybatisflex.core.paginate.Page<T> paginate) {
        Page<T> page = new Page<>();
        page.setTotalCount((int) paginate.getTotalRow());
        page.setPageNumber((int) paginate.getPageNumber());
        page.setPagesAvailable((int) paginate.getTotalPage());
        page.setPageItems(paginate.getRecords());
        return page;
    }

}
