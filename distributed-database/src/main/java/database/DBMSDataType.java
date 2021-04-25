package database;

class DBMSDataTypes {

    enum DataType {

        INTEGER(0, "int"),
        VARCHAR(1, "String");

        private int id;
        private String value;

        DataType(int id, String value) {
            this.id = id;
            this.value = value;
        }

        int getId() {
            return this.id;
        }

        String getValue() {
            return this.value;
        }

    }

    DataType getDataType(String value) {
        for(DataType dataType: DataType.values()) {
            if(dataType.getValue().equalsIgnoreCase(value)) {
                return dataType;
            }
        }
        return null;
    }
}
