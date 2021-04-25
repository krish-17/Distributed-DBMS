package database;

public class Operators {

    Operator op;

    enum Operator {
        GREATER_THAN(0, ">"),
        LESSER_THAN(1, "<"),
        GREATER_THAN_EQUALS(2, ">="),
        LESSER_THAN_EQUALS(3, "<="),
        EQUAL_TO(4, "="),
        NOT_EQUAL(5, "!=");

        private int id;
        private String operatorString;

        Operator(int id, String operatorString) {
            this.id = id;
            this.operatorString = operatorString;
        }

        String getOperatorString() {
            return this.operatorString;
        }

        int getOperatorId() {
            return this.id;
        }
    }

    Operators(Operator op) {
        this.op = op;
    }

    Operator convertStringToOperator(String operatorString) {
        for (Operator op : Operator.values()) {
            if (op.getOperatorString().equalsIgnoreCase(operatorString)) {
                return op;
            }
        }
        return null;
    }

    boolean performStringComparison(String actualValue, String expectedValue) {
        switch (this.op.getOperatorId()) {
            case 0:
            case 1:
            case 2:
            case 3:
                return false;
            case 4:
                return actualValue.equalsIgnoreCase(expectedValue);
            case 5:
                return !actualValue.equalsIgnoreCase(expectedValue);
            default:
                break;
        }
        return false;
    }

    boolean performIntComparison(int actualValue, int expectedValue) {
        switch (this.op.getOperatorId()) {
            case 0:
                return actualValue > expectedValue;
            case 1:
                return actualValue < expectedValue;
            case 2:
                return actualValue >= expectedValue;
            case 3:
                return actualValue <= expectedValue;
            case 4:
                return actualValue == expectedValue;
            case 5:
                return actualValue != expectedValue;
            default:
                return false;
        }
    }
}

