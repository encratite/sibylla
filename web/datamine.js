function renderDataMiningUI() {
	const model = getModel();
	const topLevel = createElement("div", document.body, {
		className: "container"
	});
	model.results.forEach(asset => {
		const table = createElement("table", topLevel, "assets");
		const headers = [
			"Strategy",
			"Features",
			"Side",
			"Exit",
			"Returns",
			"RAR",
			"MinRAR",
			"RecRAR",
			"Days Traded",
			"Equity Curve"
		];
		const headerRow = createElement("tr", table);
		headers.forEach(name => {
			const cell = createElement("th", headerRow);
			cell.textContent = name;
		});
		asset.strategies.forEach((strategy, index) => {
			const row = createElement("tr", table);
			const isLong = strategy.side === 0;
			const side = createElement("span");
			side.textContent = isLong ? "Long" : "Short";
			if (isLong === false) {
				side.className = "short";
			}
			const getRiskAdjusted = property => {
				return [property.toFixed(3), true];
			};
			const equityCurve = createElement("img", null, {
				src: strategy.plot
			});
			const featuresList = createElement("ul");
			strategy.features.forEach(feature => {
				const element = createElement("li", featuresList);
				element.textContent = `${feature.symbol}.${feature.name} (${feature.min}, ${feature.max})`
				featuresList.append(element);
			});
			const cells = [
				[`${asset.symbol} #${index + 1}`, false],
				[featuresList, false],
				[side, false],
				[strategy.exit, false],
				[formatMoney(strategy.returns), true],
				getRiskAdjusted(strategy.riskAdjusted),
				getRiskAdjusted(strategy.riskAdjustedMin),
				getRiskAdjusted(strategy.riskAdjustedRecent),
				[getPercentage(strategy.tradesRatio), true],
				[equityCurve, false],
			];
			cells.forEach(definition => {
				const content = definition[0];
				const isNumeric = definition[1];
				const cell = createElement("td", row);
				if (typeof content === "string") {
					cell.textContent = content;
					if (isNumeric === true) {
						cell.className = "numeric";
					}
				} else {
					cell.appendChild(content);
				}
			});
		});
	});
}

addEventListener("DOMContentLoaded", event => {
	renderDataMiningUI();
});