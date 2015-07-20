package org.apache.drill.contrib.function;

import com.google.common.base.Strings;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;


@FunctionTemplate(
        name = "mask",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class SimpleMaskFunc implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder input;

    @Param(constant = true)
    VarCharHolder mask;

    @Param(constant = true)
    IntHolder toReplace;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;


    public void setup() {
    }


    public void eval() {

        // get the value and replace with
        String maskValue = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(mask);
        String stringValue = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);

        int numberOfCharToReplace = Math.min(toReplace.value, stringValue.length());

        // build the mask substring
        String maskSubString = com.google.common.base.Strings.repeat(maskValue, numberOfCharToReplace);
        String outputValue = (new StringBuilder(maskSubString)).append(stringValue.substring(numberOfCharToReplace)).toString();

        // put the output value in the out buffer
        out.buffer = buffer;
        out.start = 0;
        out.end = outputValue.getBytes().length;
        buffer.setBytes(0, outputValue.getBytes());

    }


}


