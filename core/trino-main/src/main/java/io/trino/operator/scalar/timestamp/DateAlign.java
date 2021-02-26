package io.trino.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import static io.trino.operator.scalar.DateTimeFunctions.getFixedTimestampField;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;

@Description("align date to specified interval")
@ScalarFunction("date_align")
public class DateAlign
{
    private DateAlign() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static long align(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long interval,
            @SqlType("timestamp(p)") long timestamp)
    {
        timestamp = scaleEpochMicrosToMillis(timestamp);

        interval = interval * getFixedTimestampField(ISOChronology.getInstanceUTC(), unit).getDurationField().getUnitMillis();

        long aligned = timestamp / interval * interval;

        return scaleEpochMillisToMicros(aligned);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static LongTimestamp align(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long interval,
            @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return new LongTimestamp(
                align(unit, interval, timestamp.getEpochMicros()),
                timestamp.getPicosOfMicro());
    }
}
