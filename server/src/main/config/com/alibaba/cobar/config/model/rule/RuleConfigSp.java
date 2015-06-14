package com.alibaba.cobar.config.model.rule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.config.util.ConfigException;
import com.alibaba.cobar.util.SplitUtil;

public class RuleConfigSp {

	private final List<String> ranges;
	private final Map<String, Integer> routeTable;

	public RuleConfigSp(List<String> ranges) {
		super();
		this.ranges = ranges;

		routeTable = new HashMap<String, Integer>();

		int ii = 0;
		for (String rangeStr : ranges) {

			String[] subRanges = SplitUtil.split(rangeStr, ',', '$', '-');
			for (String tableName : subRanges) {

				if (routeTable.get(tableName) != null) {
					throw new ConfigException("rule " + tableName + " is duplicate !");
				} else {
					routeTable.put(tableName, ii);
				}

			}
			ii++;
		}

	}

	public List<String> getRanges() {
		return ranges;
	}

	public Map<String, Integer> getRouteTable() {
		return routeTable;
	}

	public static void main(String[] args) {

	}

}
