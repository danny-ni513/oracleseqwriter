package ni.danny.datax.plugin.writer.oracleseqwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author danny_ni
 */
public class OracleSeqWriter extends Writer {

    public static class Job extends Writer.Job{

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;
        private DataBaseType dataBaseType = DataBaseType.Oracle;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            OriginalConfPretreatmentUtil.DATABASE_TYPE = this.dataBaseType;
            OriginalConfPretreatmentUtil.doPretreatment(this.originalConfig,this.dataBaseType);
            LOG.debug("After job init(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        @Override
        public void prepare() {
            int tableNumber = this.originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if(tableNumber == 1){
                String username = this.originalConfig.getString(Key.USERNAME);
                String password = this.originalConfig.getString(Key.PASSWORD);

                List<Object> conns = this.originalConfig.getList(Constant.CONN_MARK,Object.class);

                Configuration connConf = Configuration.from(conns.get(0).toString());

                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                this.originalConfig.set(Key.JDBC_URL,jdbcUrl);

                String table = connConf.getList(Key.TABLE,String.class).get(0);
                this.originalConfig.set(Key.TABLE,table);

                List<String> preSqls = this.originalConfig.getList(Key.PRE_SQL,String.class);

                List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls,table);

                originalConfig.remove(Constant.CONN_MARK);

                if(null != renderedPreSqls && !renderedPreSqls.isEmpty()){

                    this.originalConfig.remove(Key.PRE_SQL);

                    Connection conn = DBUtil.getConnection(this.dataBaseType,jdbcUrl,username,password);

                    LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
                            StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

                    WriterUtil.executeSqls(conn,renderedPreSqls,jdbcUrl,this.dataBaseType);

                    DBUtil.closeDBResources(null,null,conn);
                }

            }

            LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        public void writerPreCheck(){
            prePostSqlValid();
            privilegeValid();
        }

        public void prePostSqlValid(){
            WriterUtil.preCheckPrePareSQL(this.originalConfig,this.dataBaseType);
            WriterUtil.preCheckPostSQL(this.originalConfig,this.dataBaseType);
        }

        public void privilegeValid(){
            String username = this.originalConfig.getString(Key.USERNAME);
            String password = this.originalConfig.getString(Key.PASSWORD);
            List<Object> connections = originalConfig.getList(Constant.CONN_MARK,Object.class);

            for(int i=0,len = connections.size();i < len; i++){
                Configuration connConf = Configuration.from(connections.get(i).toString());
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                List<String> expandedTables = connConf.getList(Key.TABLE,String.class);
                boolean hasInsertPri = DBUtil.checkInsertPrivilege(this.dataBaseType,jdbcUrl,username,password,expandedTables);

                if(!hasInsertPri){
                    throw RdbmsException.asInsertPriException(this.dataBaseType, this.originalConfig.getString(Key.USERNAME), jdbcUrl);
                }

                if(DBUtil.needCheckDeletePrivilege(this.originalConfig)){
                    boolean hasDeletePri = DBUtil.checkDeletePrivilege(this.dataBaseType,jdbcUrl,username,password,expandedTables);
                    if(!hasDeletePri){
                        throw RdbmsException.asDeletePriException(dataBaseType, originalConfig.getString(Key.USERNAME), jdbcUrl);
                    }
                }
            }
        }

        @Override
        public List<Configuration> split(int i) {
            return WriterUtil.doSplit(this.originalConfig,i);
        }

        @Override
        public void post() {
            int tableNumber = this.originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if(tableNumber == 1){
                String username = this.originalConfig.getString(Key.USERNAME);
                String password = this.originalConfig.getString(Key.PASSWORD);

                String jdbcUrl = this.originalConfig.getString(Key.JDBC_URL);

                String table = this.originalConfig.getString(Key.TABLE);

                List<String> postSqls = this.originalConfig.getList(Key.POST_SQL,String.class);

                List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(postSqls,table);

                if(null != renderedPostSqls && !renderedPostSqls.isEmpty()){
                    this.originalConfig.remove(Key.POST_SQL);

                    Connection conn = DBUtil.getConnection(this.dataBaseType,jdbcUrl,username,password);

                    LOG.info(
                            "Begin to execute postSqls:[{}]. context info:{}.",
                            StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
                    WriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);

                }
            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Writer.Task{
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig = null;
        protected DataBaseType dataBaseType = DataBaseType.Oracle;
        private static final String VALUE_HOLDER = "?";

        protected String username;
        protected String password;
        protected String jdbcUrl;
        protected String table;
        protected List<OracleColumnCell> columns;
        protected List<String> columnNames;
        protected boolean needOrder = false;
        protected List<String> preSqls;
        protected List<String> postSqls;
        protected int batchSize;
        protected int batchByteSize;
        protected int columnNumber = 0;
        protected int valueColumnNumber = 0;
        protected int whereColumnNumber = 0;
        protected int seqColumnNumber = 0;
        protected TaskPluginCollector taskPluginCollector;

        protected static String BASIC_MESSAGE;

        protected static String EXECUTE_SQL_TEMPLATE;
        protected static String INSERT_OR_REPLACE_TEMPLATE;
        protected String writeRecordSql;
        protected String writeMode;
        protected boolean emptyAsNull;
        protected Triple<List<String>,List<Integer>,List<String>> resultSetMetaData;


        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.username = this.writerSliceConfig.getString(Key.USERNAME);
            this.password = this.writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = this.writerSliceConfig.getString(Key.JDBC_URL);

            this.table = this.writerSliceConfig.getString(Key.TABLE);
            List<Object> columnTmpList = this.writerSliceConfig.getList(Key.COLUMN);

            this.columns = DBUtil.parseColumn(columnTmpList);
            List tmpColumnNames  = new ArrayList<>(this.columns.size());
            for(OracleColumnCell columnCell : this.columns){
                tmpColumnNames.add(columnCell.getColumnName());
            }
            this.columnNames = tmpColumnNames;

            this.needOrder = checkNeedOrder();
            this.columnNumber = this.columns.size();
            this.valueColumnNumber = calcColumnNumberByType(ColumnType.VALUE);
            this.whereColumnNumber = calcColumnNumberByType(ColumnType.WHERE);
            this.seqColumnNumber = calcColumnNumberByType(ColumnType.SEQ);

            this.preSqls = this.writerSliceConfig.getList(Key.PRE_SQL,String.class);
            this.postSqls = this.writerSliceConfig.getList(Key.POST_SQL,String.class);
            this.batchSize = this.writerSliceConfig.getInt(Key.BATCH_SIZE,Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = this.writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE,Constant.DEFAULT_BATCH_BYTE_SIZE);

            this.writeMode = this.writerSliceConfig.getString(Key.WRITE_MODE,Constant.DEFAULT_WRITE_MODE);

            this.emptyAsNull = this.writerSliceConfig.getBool(Key.EMPTY_AS_NULL,Constant.DEFAULT_EMPTY_AS_NULL);

            INSERT_OR_REPLACE_TEMPLATE = this.writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);

            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE,this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
                    this.jdbcUrl, this.table);
        }

        private boolean checkNeedOrder(){
            for(OracleColumnCell columnCell :this.columns){
                if(columnCell.getColumnIndex() != -1){
                    return true;
                }
            }
            return false;
        }

        private int calcColumnNumberByType(ColumnType type){
            int num = 0;
            for(OracleColumnCell columnCell :this.columns){
                if(type.equals(columnCell.getColumnType())){
                    num++;
                }
            }
            return num;
        }

        //准备SQL 模版
        //初始化DATASOURCE
        @Override
        public void prepare() {
            Connection connection = DBUtil.getConnection(this.dataBaseType,this.jdbcUrl,this.username,this.password);

            DBUtil.dealWithSessionConfig(connection,this.writerSliceConfig,this.dataBaseType,BASIC_MESSAGE);

            int tableNumber = this.writerSliceConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if(tableNumber != 1){
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
                        StringUtils.join(this.preSqls, ";"), BASIC_MESSAGE);
                WriterUtil.executeSqls(connection, this.preSqls, BASIC_MESSAGE, dataBaseType);
            }

            DBUtil.closeDBResources(null, null, connection);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            this.taskPluginCollector = super.getTaskPluginCollector();
            Connection connection = DBUtil.getConnection(this.dataBaseType,this.jdbcUrl,this.username,this.password);
            DBUtil.dealWithSessionConfig(connection,writerSliceConfig,this.dataBaseType,BASIC_MESSAGE);
            startWriteWithConnection(recordReceiver,connection);
        }

        public void startWriteWithConnection(RecordReceiver recordReceiver,Connection connection){

            this.resultSetMetaData = DBUtil.getColumnMetaData(connection,this.table,StringUtils.join(this.columnNames,","));

            calcWriteRecordSql();
            LOG.info("writeRecordSql is ==>[{}]",this.writeRecordSql);

            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            int bufferBytes = 0;
            try{
                Record record;
                while((record = recordReceiver.getFromReader()) !=null){
                    if(record.getColumnNumber() != this.columnNumber-this.seqColumnNumber){
                        //读取源头字段与目的表字段写入列数(减去自增列)不想等，直接报错
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                this.columnNumber));
                    }

                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if(writeBuffer.size() >= batchSize ||bufferBytes >= batchByteSize){
                        doBatchInsert(connection,writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }

                }
                if(!writeBuffer.isEmpty()){
                    doBatchInsert(connection,writeBuffer);
                    writeBuffer.clear();
                    bufferBytes = 0;

                }
            }catch (Exception ex){
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, ex);
            }finally {
                writeBuffer.clear();
                bufferBytes = 0;
                DBUtil.closeDBResources(null, null, connection);
            }
        }

        public void doBatchInsert(Connection connection, List<Record> buffer) throws SQLException {
            PreparedStatement preparedStatement = null;
            try{
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(this.writeRecordSql);

                for(Record record : buffer){
                    preparedStatement = fillPreparedStatement(
                            preparedStatement, record);
                    preparedStatement.addBatch();
                }

                preparedStatement.executeBatch();
                connection.commit();
            }catch (SQLException e){
                LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer);
            }catch (Exception e){
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }



        public void doOneInsert(Connection connection,List<Record> buffer){
            PreparedStatement preparedStatement = null;
            try{
                connection.setAutoCommit(true);
                preparedStatement = connection.prepareStatement(this.writeRecordSql);
                for(Record record : buffer){
                    try{
                        preparedStatement = fillPreparedStatement(preparedStatement,record);
                        preparedStatement.execute();
                    }catch (SQLException e){
                        LOG.debug(e.toString());

                        this.taskPluginCollector.collectDirtyRecord(record, e);
                    }finally {
                        // 最后不要忘了关闭 preparedStatement
                        preparedStatement.clearParameters();
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }

        }

        protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement,Record record) throws SQLException{
            int i=0;
            for (OracleColumnCell columnCell :this.columns) {
                if(ColumnType.VALUE.equals(columnCell.getColumnType())||ColumnType.WHERE.equals(columnCell.getColumnType())){
                    int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
                    preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, record.getColumn(i));
                    i++;
                }
            }

            return preparedStatement;
        }

        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column) throws SQLException {
            java.util.Date utilDate;
            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    preparedStatement.setString(columnIndex + 1, column
                            .asString());
                    break;

                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    String strValue = column.asString();
                    if (emptyAsNull && "".equals(strValue)) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, strValue);
                    }
                    break;

                //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
                case Types.TINYINT:
                    Long longValue = column.asLong();
                    if (null == longValue) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, longValue.toString());
                    }
                    break;

                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                case Types.DATE:
                    if (this.resultSetMetaData.getRight().get(columnIndex)
                            .equalsIgnoreCase("year")) {
                        if (column.asBigInteger() == null) {
                            preparedStatement.setString(columnIndex + 1, null);
                        } else {
                            preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                        }
                    } else {
                        java.sql.Date sqlDate = null;
                        try {
                            utilDate = column.asDate();
                        } catch (DataXException e) {
                            throw new SQLException(String.format(
                                    "Date 类型转换错误：[%s]", column));
                        }

                        if (null != utilDate) {
                            sqlDate = new java.sql.Date(utilDate.getTime());
                        }
                        preparedStatement.setDate(columnIndex + 1, sqlDate);
                    }
                    break;

                case Types.TIME:
                    java.sql.Time sqlTime = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIME 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                    }
                    preparedStatement.setTime(columnIndex + 1, sqlTime);
                    break;

                case Types.TIMESTAMP:
                    java.sql.Timestamp sqlTimestamp = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIMESTAMP 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTimestamp = new java.sql.Timestamp(
                                utilDate.getTime());
                    }
                    preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    preparedStatement.setBytes(columnIndex + 1, column
                            .asBytes());
                    break;

                case Types.BOOLEAN:
                    preparedStatement.setString(columnIndex + 1, column.asString());
                    break;

                // warn: bit(1) -> Types.BIT 可使用setBoolean
                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                case Types.BIT:
                    if (this.dataBaseType == DataBaseType.MySql) {
                        preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                    } else {
                        preparedStatement.setString(columnIndex + 1, column.asString());
                    }
                    break;
                default:
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.UNSUPPORTED_TYPE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                            this.resultSetMetaData.getLeft()
                                                    .get(columnIndex),
                                            this.resultSetMetaData.getMiddle()
                                                    .get(columnIndex),
                                            this.resultSetMetaData.getRight()
                                                    .get(columnIndex)));
            }
            return preparedStatement;
        }

        private void calcWriteRecordSql() {
            if (!VALUE_HOLDER.equals(calcValueHolder(""))) {
                List<String> valueHolders = new ArrayList<String>(columnNumber);
                for (int i = 0; i < columns.size(); i++) {
                    String type = resultSetMetaData.getRight().get(i);
                    valueHolders.add(calcValueHolder(type));
                }

                boolean forceUseUpdate = false;
                //ob10的处理
                if (dataBaseType != null && dataBaseType == DataBaseType.MySql && OriginalConfPretreatmentUtil.isOB10(jdbcUrl)) {
                    forceUseUpdate = true;
                }

                if(this.needOrder){
                    Collections.sort(columns);
                }
                INSERT_OR_REPLACE_TEMPLATE = WriterUtil.getWriteTemplate(columns, VALUE_HOLDER, writeMode, dataBaseType, forceUseUpdate);
                writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);
            }
        }

        protected String calcValueHolder(String columnType) {
            return VALUE_HOLDER;
        }

        @Override
        public void post() {
            int tableNumber = this.writerSliceConfig.getInt(Constant.TABLE_NUMBER_MARK);
            boolean hasPostSql = (this.postSqls !=null && this.postSqls.size()>0);
            if(tableNumber == 1 || !hasPostSql){
                return;
            }

            Connection connection = DBUtil.getConnection(this.dataBaseType,this.jdbcUrl,this.username,this.password);


            LOG.info("Begin to execute postSqls:[{}]. context info:{}.",
                    StringUtils.join(this.postSqls, ";"), BASIC_MESSAGE);
            WriterUtil.executeSqls(connection, this.postSqls, BASIC_MESSAGE, dataBaseType);
            DBUtil.closeDBResources(null, null, connection);

        }

        @Override
        public void destroy() {

        }
    }
}
