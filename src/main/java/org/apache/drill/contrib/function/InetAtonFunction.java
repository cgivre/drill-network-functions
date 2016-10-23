package org.apache.drill.contrib.function;

import com.google.common.base.Strings;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;


@FunctionTemplate(
        name = "inet_aton",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class InetAtonFunction implements DrillSimpleFunc {

    @Param 
	NullableVarCharHolder inputTextA;

	@Output BigIntHolder out;

  	@Inject
	DrillBuf buffer;


    public void setup() {
    }


    public void eval() {
		String ip_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);

		String[] ipAddressInArray = ip_string.split("\\.");

		long result = 0;
		for (int i = 0; i < ipAddressInArray.length; i++) {
			int power = 3 - i;
			int ip = Integer.parseInt(ipAddressInArray[i]);
			result += ip * Math.pow(256, power);

		}

		out.value = result;
    }


}


