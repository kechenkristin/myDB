package simpledb.storage;

public class Table {
    private int tableId;
    private DbFile dbFile;
    private String tableName;
    private String pkName;


    public String getPkName() {
        return pkName;
    }

    public void setPkName(String pkName) {
        this.pkName = pkName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public DbFile getDbFile() {
        return dbFile;
    }

    public void setDbFile(DbFile dbFile) {
        this.dbFile = dbFile;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public Table(DbFile dbFile, String tableName, String pkName) {
        this.dbFile = dbFile;
        this.tableName = tableName;
        this.pkName = pkName;
        this.tableId = dbFile.getId();
    }
}
