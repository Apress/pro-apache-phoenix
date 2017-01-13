package com.apress.phoenix.chapter7;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

import java.sql.SQLException;
import java.util.List;

/**
 *
 */
public class HasVowelsFunction extends ScalarFunction  {

    private static final String FUNC_NAME = "hasVowels";

    public HasVowelsFunction() {}

    public HasVowelsFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public String getName() {
        return FUNC_NAME;
    }

    /**
     * Determines whether or not a function may be used to form
     * the start/stop key of a scan
     * @return the zero-based position of the argument to traverse
     * into to look for a primary key column reference, or
     * {@value #NO_TRAVERSAL} if the function cannot be used to
     * form the scan key.
     */
    public int getKeyFormationTraversalIndex() {
        return NO_TRAVERSAL;
    }

    /**
     * Manufactures a KeyPart used to construct the KeyRange given
     * a constant and a comparison operator.
     * @param childPart the KeyPart formulated for the child expression
     * at the {@link #getKeyFormationTraversalIndex()} position.
     * @return the KeyPart for constructing the KeyRange for this function.
     */
    public KeyPart newKeyPart(KeyPart childPart) {
        return null;
    }

    /**
     * Determines whether or not the result of the function invocation
     * will be ordered in the same way as the input to the function.
     * Returning YES enables an optimization to occur when a
     * GROUP BY contains function invocations using the leading PK
     * column(s).
     * @return YES if the function invocation will always preserve order for
     * the inputs versus the outputs and false otherwise, YES_IF_LAST if the
     * function preserves order, but any further column reference would not
     * continue to preserve order, and NO if the function does not preserve
     * order.
     */
    public OrderPreserving preservesOrder() {
        return OrderPreserving.NO;
    }

    /**
     * is the method to be implemented which provides access to the Tuple
     * @param tuple Single row result during scan iteration
     * @param ptr Pointer to byte value being accessed
     * @return
     */
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression child = children.get(0);
        if (!child.evaluate(tuple, ptr)) {
            return false;
        }
        String inputStr = (String) PVarchar.INSTANCE.toObject(ptr, child.getSortOrder());
        if (inputStr == null) {
            return true;
        }
        boolean vowelFound = false;
        for(char each : inputStr.toCharArray()) {
            if(vowelFound) {
                break;
            }
            switch(each) {
                case 'a':
                case 'e':
                case 'i':
                case 'o':
                case 'u':
                    ptr.set(PBoolean.INSTANCE.toBytes(true));
                    vowelFound = true;
                    break;
                default:
            }
        }
        if(!vowelFound) {
            ptr.set(PBoolean.INSTANCE.toBytes(false));
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PBoolean.INSTANCE;
    }
}
