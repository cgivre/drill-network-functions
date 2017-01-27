package org.apache.drill.contrib.function;


import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;

import javax.inject.Inject;


@FunctionTemplate(
        name = "is_private_ip",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class IsPrivateIP implements DrillSimpleFunc {

    @Param 
	NullableVarCharHolder inputTextA;

	@Output BitHolder out;

  	@Inject
	DrillBuf buffer;


    public void setup() {
    }


    public void eval() {
		String ip_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);

		String[] ipAddressInArray = ip_string.split("\\.");
		
		int result = 0;

		int[] octets = new int[3];	

		for( int i = 0; i < 3; i++ ){
			octets[i] = Integer.parseInt( ipAddressInArray[i] );
			if( octets[i] > 255 || octets[i] < 0 ) {
				result = 0;
			}
		}	
		
		if( octets[0] == 192 && octets[1] == 168 ) {
			result = 1;
		}
		else if (octets[0] == 172 && octets [1] >= 16 && octets[1] <= 31 ){
			result = 1;
		}
		else if( octets[0] == 10 ) {
			result = 1;
		}
		else {
			result = 0;
		}

		out.value = result;
    }


}


