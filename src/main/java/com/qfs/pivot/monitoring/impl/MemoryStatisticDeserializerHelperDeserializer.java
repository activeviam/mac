package com.qfs.pivot.monitoring.impl;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil.MemoryStatisticDeserializerHelper;
import com.quartetfs.fwk.impl.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryStatisticDeserializerHelperDeserializer
		extends JsonDeserializer<MemoryStatisticDeserializerHelper> {

	@Override public MemoryStatisticDeserializerHelper deserialize(
			JsonParser jsonParser, DeserializationContext deserializationContext)
			throws IOException, JacksonException {

		final Map<Integer, List<Pair<Long, Long>>> rangeMap = new HashMap<>();
		final AtomicLong length = new AtomicLong();
		if (!JsonToken.START_OBJECT.equals(jsonParser.currentToken())) {
			throw new IllegalArgumentException("Should be called at the start of an object");
		}

		final AtomicInteger depth = new AtomicInteger();
		final Map<Integer, Long> lastOpeningPos = new HashMap<>();
		do {
			final JsonToken curToken = jsonParser.currentToken();

			if (curToken.equals(JsonToken.START_OBJECT)) {
				long openingPos = jsonParser.currentLocation().getCharOffset();
				lastOpeningPos.put(depth.incrementAndGet(), openingPos);
			} else if (curToken.equals(JsonToken.END_OBJECT)) {
				long closingPos = jsonParser.currentLocation().getCharOffset();
				length.set(closingPos);
				//Create Range Info : <[startPos,endPos],depth>
				final int curDepth = depth.getAndDecrement();
				long previousMatchingOpeningPos = lastOpeningPos.get(curDepth);
				final Pair<Long, Long> curRange = new Pair<>(previousMatchingOpeningPos, closingPos);
				rangeMap.compute(curDepth, (k, v) -> {
					final List<Pair<Long, Long>> list;
					if (v == null) {
						list = new ArrayList<>();
					} else {
						list = v;
					}
					list.add(curRange);
					return list;
				});
				//Clear the lastOpeningPos for the curDepth value
				lastOpeningPos.remove(curDepth);
			}
			jsonParser.nextToken();
		} while (!Objects.equals(jsonParser.currentToken(), null));

		return new MemoryStatisticDeserializerHelper(rangeMap, length.get());
	}
}
