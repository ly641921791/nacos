package com.alibaba.nacos.plugin.datasource.impl.oracle12c;

import com.alibaba.nacos.common.utils.ArrayUtils;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;

import java.util.List;

public abstract class AbstractOracleMapper extends AbstractMapper {

    @Override
    public String select(List<String> columns, List<String> where, Object[] args) {
        if (ArrayUtils.isEmpty(args)) {
            return select(columns, where);
        }

        StringBuilder sql = new StringBuilder();
        String method = "SELECT ";
        sql.append(method);
        for (int i = 0; i < columns.size(); i++) {
            sql.append(columns.get(i));
            if (i == columns.size() - 1) {
                sql.append(" ");
            } else {
                sql.append(",");
            }
        }
        sql.append("FROM ");
        sql.append(getTableName());
        sql.append(" ");

        if (CollectionUtils.isEmpty(where)) {
            return sql.toString();
        }
        if (where.size() != args.length) {
            throw new IllegalArgumentException("The where.size need to equals args.length");
        }

        sql.append("WHERE ");
        for (int i = 0; i < where.size(); i++) {
            Object arg = args[i];
            if ("".equals(arg)) {
                // When arg is empty string, the sql should be "xxx IS NULL".
                // But when XxxPersistServiceImpl execute sql, the arg is input to execute.
                // To be compatible with this approach, sql is generated to "(xxx IS NULL OR xxx = '')"
                sql.append("(").append(where.get(i)).append(" IS NULL OR ").append(where.get(i)).append(" = ?)");
            } else {
                sql.append(where.get(i)).append(" = ").append("?");
            }

            if (i != where.size() - 1) {
                sql.append(" AND ");
            }
        }
        return sql.toString();
    }

}
