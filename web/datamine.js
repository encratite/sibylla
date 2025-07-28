function renderDataMiningUI() {
	const model = getModel();
	const container = createElement("div", document.body, {
		className: "container-data-mine"
	});
	model.results.forEach(asset => {
		const header = createElement("h1", container);
		header.textContent = `${asset.symbol} (${asset.strategies.length} Strategies)`;
		let tableContainer = null;
		asset.strategies.forEach((strategy, index) => {
			if (index % 2 === 0) {
				tableContainer = createElement("div", container, "strategy");
			}
			const table = createElement("table", tableContainer);
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
			const features = strategy.features.map(feature => {
				return `${feature.symbol}.${feature.name} (${feature.min}, ${feature.max})`;
			});
			const timeOfDay = strategy.timeOfDay != null ? strategy.timeOfDay : "-";
			const cells1 = [
				["Strategy", strategyName, false],
				["Feature 1", features[0], false],
				["Feature 2", features[1], false],
				["Side", side, false],
				["Time", timeOfDay, false],
				["Exit", strategy.exit, false],
			];
			const cells2 = [
				["Returns", formatMoney(strategy.returns), true],
				getRiskAdjusted("RAR", strategy.riskAdjusted),
				getRiskAdjusted("MinRAR", strategy.riskAdjustedMin),
				getRiskAdjusted("RecRAR", strategy.riskAdjustedRecent),
				["Max Drawdown", getPercentage(strategy.maxDrawdown), true],
				["Days Traded", getPercentage(strategy.tradesRatio), true],
			];
			const renderCell = (definition, row) => {
				const description = definition[0];
				const content = definition[1];
				const isNumeric = definition[2];
				const descriptionCell = createElement("td", row, "description");
				descriptionCell.textContent = description;
				const contentCell = createElement("td", row);
				if (typeof content === "string") {
					contentCell.textContent = content;
					if (isNumeric === true) {
						contentCell.classList.add("numeric");
					}
				} else {
					contentCell.appendChild(content);
				}
			};
			for (let i = 0; i < cells1.length; i++) {
				const row = createElement("tr", table);
				renderCell(cells1[i], row);
				renderCell(cells2[i], row);
			}
			const plotRow = createElement("tr", table);
			const equityCurveCell = createElement("td", plotRow, {
				className: "plot",
				colSpan: cells1.length,
			});
			equityCurveCell.appendChild(equityCurve);
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