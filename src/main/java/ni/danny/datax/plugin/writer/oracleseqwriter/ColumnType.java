package ni.danny.datax.plugin.writer.oracleseqwriter;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public enum ColumnType {
    SEQ("seq"),
    VALUE("value"),
    WHERE("where"),
    CONST("const"),
    DATE("date"),
    WHERE_DATE("where_date"),
    WHERE_CONST("where_const")
    ;
    private String typeName;

    ColumnType(String typeName){
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName) {
        if(StringUtils.isBlank(typeName)){
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("oracleseqwriter 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
        }
        for (ColumnType columnType : values()) {
            if (StringUtils.equalsIgnoreCase(columnType.typeName, typeName.trim())) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                String.format("oracleseqwriter 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString() {
        return this.typeName;
    }

}
