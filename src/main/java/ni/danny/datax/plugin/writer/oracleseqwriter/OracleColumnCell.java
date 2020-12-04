package ni.danny.datax.plugin.writer.oracleseqwriter;

import com.alibaba.datax.common.base.BaseObject;

import java.io.Serializable;

/**
 * @author danny_ni
 */
public class OracleColumnCell extends BaseObject implements Serializable, Comparable<OracleColumnCell> {
    private ColumnType columnType;
    private String columnName;
    private int columnIndex;
    private String seqName;

    public ColumnType getColumnType() {
        return columnType;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getSeqName() {
        return seqName;
    }

    private OracleColumnCell(Builder builder){
        this.columnType = builder.columnType;
        this.columnIndex = builder.columnIndex;
        this.columnName = builder.columnName;
        this.seqName = builder.seqName;
    }

    @Override
    public int compareTo(OracleColumnCell o) {
        if(o != null){
            return o.getColumnIndex() - getColumnIndex();
        }
        return 0;
    }

    public static class Builder {
        private ColumnType columnType;
        private String columnName;
        private int columnIndex;
        private String seqName;

        public Builder setSeqName(String seqName) {
            this.seqName = seqName;
            return this;
        }



        public Builder setColumnType(ColumnType columnType) {
            this.columnType = columnType;
            return this;
        }

        public Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
            return this;
        }

        public OracleColumnCell build() {
            return new OracleColumnCell(this);
        }
    }

}
