function renderDataMiningUI() {
	const model = getModel();
	const container = createElement("div", document.body, {
		className: "container-data-mine"
	});
	model.results.forEach(asset => {
		const header = createElement("h1", container);
		header.textContent = `${asset.symbol} Strategies`;
		const table = createElement("table", container, "assets");
		asset.strategies.forEach((strategy, index) => {
			const isLong = strategy.side === 0;
			const side = createElement("span");
			side.textContent = isLong ? "Long" : "Short";
			if (isLong === false) {
				side.className = "short";
			}
			const getRiskAdjusted = (description, property) => {
				return [description, property.toFixed(3), true];
			};
			const strategyName = `${asset.symbol} #${index + 1}`;
			const equityCurve = createElement("img", null, {
				src: strategy.plot,
				className: "equityCurve",
				onclick: () => showEquityCurve(strategyName, strategy),
			});
			const weekdayPlot = createElement("img", null, {
				src: strategy.weekdayPlot,
				className: "weekdayPlot",
				onclick: () => showWeekdayPlot(strategyName, strategy),
			});
			const featureNames = strategy.features.map(feature => {
				return `${feature.symbol}.${feature.name}`;
			});
			const featureThresholds = strategy.features.map(feature => {
				return `${feature.min} - ${feature.max}`;
			});
			const timeOfDay = strategy.timeOfDay != null ? strategy.timeOfDay : "-";
			const cells = [
				["Strategy", strategyName],
				["Feature 1", featureNames[0]],
				["Threshold 1", featureThresholds[0]],
				["Feature 2", featureNames[1]],
				["Threshold 2", featureThresholds[1]],
				["Side", side],
				["Time", timeOfDay],
				["Exit", strategy.exit],
				["Returns", formatMoney(strategy.returns)],
				getRiskAdjusted("RAR", strategy.riskAdjusted),
				getRiskAdjusted("MinRAR", strategy.riskAdjustedMin),
				getRiskAdjusted("RecRAR", strategy.riskAdjustedRecent),
				["Max Drawdown", getPercentage(strategy.maxDrawdown)],
				["Days Traded", getPercentage(strategy.tradesRatio)],
			];
			cells.forEach((definition, index) => {
				const description = `${definition[0]}:`;
				const content = definition[1];
				const isNumeric = definition[2];
				const row = createElement("tr", table);
				const descriptionCell = createElement("td", row, "description");
				descriptionCell.textContent = description;
				const contentCell = createElement("td", row, "value");
				if (typeof content === "string") {
					contentCell.textContent = content;
					if (isNumeric === true) {
						contentCell.classList.add("numeric");
					}
				} else {
					contentCell.appendChild(content);
				}
				if (index === 0) {
					row.classList.add("firstRow");
					const equityCurveCell = createElement("td", row, {
						className: "plot",
						rowSpan: cells.length,
					});
					equityCurveCell.appendChild(equityCurve);
				}
			});
			if (index < asset.strategies.length - 1) {
				const paddingRow = createElement("tr", table);
				createElement("td", paddingRow, "padding");
			}
		});
	});
}

function showImage(title, src, width, height, padding) {
	width += padding;
	height += padding;
	const left = 100;
	const top = 100;
	const equityCurve = window.open("", "_blank", `width=${width},height=${height},left=${left},top=${top},resizable=yes`);
	equityCurve.document.write(`
		<!doctype html>
			<head>
				<title>${title}</title>
			</head>
		</html>
	`);
	equityCurve.document.close();
	const image = createElement("img", equityCurve.document.body, {
		src: src
	});
}

function showEquityCurve(strategyName, strategy) {
	showImage(`${strategyName} - Equity Curve`, strategy.plot, 1152, 768, 35);
}

function showWeekdayPlot(strategyName, strategy) {
	showImage(`${strategyName} - Returns by Weekday`, strategy.weekdayPlot, 432, 288, 35);
}

addEventListener("DOMContentLoaded", event => {
	renderDataMiningUI();
});