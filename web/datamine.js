function renderDataMiningUI() {
	const model = getModel();
	const topLevel = createElement("div", document.body, {
		className: "container-data-mine"
	});
	model.results.forEach(asset => {
		const table = createElement("table", topLevel, "assets");
		const headers = [
			"Strategy",
			"Features",
			"Side",
			"Time",
			"Exit",
			"Returns",
			"RAR",
			"MinRAR",
			"RecRAR",
			"Max Drawdown",
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
			const strategyName = `${asset.symbol} #${index + 1}`;
			const equityCurve = createElement("img", null, {
				src: strategy.plot,
				onclick: () => showEquityCurve(strategyName, strategy),
			});
			const featuresList = createElement("ul");
			strategy.features.forEach(feature => {
				const element = createElement("li", featuresList);
				element.textContent = `${feature.symbol}.${feature.name} (${feature.min}, ${feature.max})`
				featuresList.append(element);
			});
			const timeOfDay = strategy.timeOfDay != null ? strategy.timeOfDay : "-";
			const cells = [
				[strategyName, false],
				[featuresList, false],
				[side, false],
				[timeOfDay, false],
				[strategy.exit, false],
				[formatMoney(strategy.returns), true],
				getRiskAdjusted(strategy.riskAdjusted),
				getRiskAdjusted(strategy.riskAdjustedMin),
				getRiskAdjusted(strategy.riskAdjustedRecent),
				[getPercentage(strategy.maxDrawdown), true],
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

function showEquityCurve(strategyName, strategy) {
	const padding = 35;
	const width = 1152 + padding;
	const height = 768 + padding;
	const left = 100;
	const top = 100;
	const equityCurve = window.open("", "_blank", `width=${width},height=${height},left=${left},top=${top},resizable=yes`);
	equityCurve.document.write(`
		<!doctype html>
			<head>
				<title>${strategyName}</title>
			</head>
		</html>
	`);
	equityCurve.document.close();
	const image = createElement("img", equityCurve.document.body, {
		src: strategy.plot
	});
}

addEventListener("DOMContentLoaded", event => {
	renderDataMiningUI();
});