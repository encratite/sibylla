function renderDataMiningUI() {
	const model = getModel();
	const topLevel = createElement("div", document.body, {
		className: "container"
	});
	const table = createElement("table", topLevel, "assets");
	const headers = [
		"Asset",
		"Side",
		"Feature 1",
		"Feature 2",
		"Exit",
		"Returns",
		"Risk-Adjusted",
		"Days Traded",
		"Equity Curve"
	];
	const headerRow = createElement("tr", table);
	headers.forEach(name => {
		const cell = createElement("th", headerRow);
		cell.textContent = name;
	});
	model.results.forEach(asset => {
		asset.strategies.forEach(strategy => {
			const row = createElement("tr", table);
			const isLong = strategy.side === 0;
			const side = createElement("span");
			side.textContent = isLong ? "Long" : "Short";
			if (isLong === false) {
				side.className = "short";
			}
			const getFeatureContent = index => {
				const feature = strategy.features[index];
				return `${feature.symbol}.${feature.name} (${feature.min} - ${feature.max})`;
			};
			const equityCurve = createElement("img", null, {
				src: strategy.plot
			});
			const cells = [
				[asset.symbol, false],
				[side, false],
				[getFeatureContent(0), false],
				[getFeatureContent(1), false],
				[strategy.exit, false],
				[formatMoney(strategy.returns), true],
				[strategy.riskAdjusted.toFixed(3), true],
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