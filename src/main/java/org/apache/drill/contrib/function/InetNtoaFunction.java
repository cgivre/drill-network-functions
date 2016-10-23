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
        name = "inet_ntoa",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class InetNtoaFunction implements DrillSimpleFunc {

    @Param 
	BigIntHolder in1;

	@Output 
	VarCharHolder out;

  	@Inject
	DrillBuf buffer;


    public void setup() {
    }


    public void eval() {
		StringBuilder result = new StringBuilder(15);
					
		long inputInt = in1.value;

		for (int i = 0; i < 4; i++) {

			result.insert(0,Long.toString(inputInt & 0xff));

			if (i < 3) {
				result.insert(0,'.');
			}

			inputInt = inputInt >> 8;
		}
		
		String outputValue = result.toString();

		out.buffer = buffer;
    	out.start = 0;
    	out.end = outputValue.getBytes().length;
    	buffer.setBytes(0, outputValue.getBytes());
    }


}


