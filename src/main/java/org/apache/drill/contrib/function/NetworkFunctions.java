package org.apache.drill.contrib.function;

import com.google.common.base.Strings;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.commons.net.util.SubnetUtils;

import javax.inject.Inject;
public class NetworkFunctions{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NetworkFunctions.class);

    private NetworkFunctions() {}

    @FunctionTemplate(
            name = "in_network",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class InNetworkFunction implements DrillSimpleFunc {

        @Param
        VarCharHolder input_ip;

        @Param
        VarCharHolder input_cidr;

        @Output
        BitHolder out;

        @Inject
        DrillBuf buffer;

        @Workspace
        SubnetUtils utils;

        public void setup() {
        }


        public void eval() {

            String ip_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input_ip.start, input_ip.end, input_ip.buffer);
            String cidr_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input_cidr.start, input_cidr.end, input_cidr.buffer);

            int result = 0;
            utils = new org.apache.commons.net.util.SubnetUtils(cidr_string);

            if( utils.getInfo().isInRange( ip_string ) ){
                result = 1;
            }
            else{
                result = 0;
            }

            out.value = result;

        }


    }

    @FunctionTemplate(
            name = "getAddressCount",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class getAddressCountFunction implements DrillSimpleFunc {

        @Param
        VarCharHolder input_cidr;

        @Output
        BigIntHolder out;

        @Inject
        DrillBuf buffer;

        @Workspace
        SubnetUtils utils;

        public void setup() {
        }

        public void eval() {

            String cidr_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input_cidr.start, input_cidr.end, input_cidr.buffer);
            utils = new org.apache.commons.net.util.SubnetUtils(cidr_string);

            out.value = utils.getInfo().getAddressCount();

        }

    }

    @FunctionTemplate(
            name = "getBroadcastAddress",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class getBroadcastAddressFunction implements DrillSimpleFunc {

        @Param
        VarCharHolder input_cidr;

        @Output
        VarCharHolder out;

        @Inject
        DrillBuf buffer;

        @Workspace
        SubnetUtils utils;

        public void setup() {
        }

        public void eval() {

            String cidr_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input_cidr.start, input_cidr.end, input_cidr.buffer);
            utils = new org.apache.commons.net.util.SubnetUtils(cidr_string);

            String outputValue = utils.getInfo().getBroadcastAddress();

            out.buffer = buffer;
            out.start = 0;
            out.end = outputValue.getBytes().length;
            buffer.setBytes(0, outputValue.getBytes());

        }

    }

    @FunctionTemplate(
            name = "getNetmask",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class getNetmaskFunction implements DrillSimpleFunc {

        @Param
        VarCharHolder input_cidr;

        @Output
        VarCharHolder out;

        @Inject
        DrillBuf buffer;

        @Workspace
        SubnetUtils utils;

        public void setup() {
        }

        public void eval() {

            String cidr_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input_cidr.start, input_cidr.end, input_cidr.buffer);
            utils = new org.apache.commons.net.util.SubnetUtils(cidr_string);

            String outputValue = utils.getInfo().getNetmask();

            out.buffer = buffer;
            out.start = 0;
            out.end = outputValue.getBytes().length;
            buffer.setBytes(0, outputValue.getBytes());

        }

    }

    @FunctionTemplate(
            name = "getLowAddress",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class getLowAddressFunction implements DrillSimpleFunc {

        @Param
        VarCharHolder input_cidr;

        @Output
        VarCharHolder out;

        @Inject
        DrillBuf buffer;

        @Workspace
        SubnetUtils utils;

        public void setup() {
        }

        public void eval() {

            String cidr_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input_cidr.start, input_cidr.end, input_cidr.buffer);
            utils = new org.apache.commons.net.util.SubnetUtils(cidr_string);

            String outputValue = utils.getInfo().getLowAddress();

            out.buffer = buffer;
            out.start = 0;
            out.end = outputValue.getBytes().length;
            buffer.setBytes(0, outputValue.getBytes());

        }

    }

    @FunctionTemplate(
            name = "getHighAddress",
            scope = FunctionTemplate.FunctionScope.SIMPLE,
            nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )
    public static class getHighddressFunction implements DrillSimpleFunc {

        @Param
        VarCharHolder input_cidr;

        @Output
        VarCharHolder out;

        @Inject
        DrillBuf buffer;

        @Workspace
        SubnetUtils utils;

        public void setup() {
        }

        public void eval() {

            String cidr_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input_cidr.start, input_cidr.end, input_cidr.buffer);
            utils = new org.apache.commons.net.util.SubnetUtils(cidr_string);

            String outputValue = utils.getInfo().getHighAddress();

            out.buffer = buffer;
            out.start = 0;
            out.end = outputValue.getBytes().length;
            buffer.setBytes(0, outputValue.getBytes());

        }
    }
}