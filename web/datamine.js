function renderDataMiningUI() {
	const model = getModel();
	const container = createElement("div", document.body, {
		className: "containerDataMine"
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
			const getRiskAdjusted = (description, property) => {
				return [description, property.toFixed(3), true];
			};
			let strategyName = `${asset.symbol} #${index + 1}`;
			const equityCurve = createElement("img", null, {
				src: strategy.plot,
				className: "equityCurve",
				onclick: () => showStrategyDetails(strategyName, strategy),
			});
			const truncateThreshold = threshold => {
				const precision = 100;
				return Math.round(precision * threshold) / precision;
			};
			const features = strategy.features.map(feature => {
				const min = truncateThreshold(feature.min);
				const max = truncateThreshold(feature.max);
				return `${feature.symbol}.${feature.name} (${min}, ${max})`;
			});
			const side = strategy.side === 0 ? "Long" : "Short";
			const options = [
				side
			];
			if (strategy.optimizeWeekdays === true) {
				options.push("weekday optimization");
			}
			const optionsString = options.join(", ");
			const timeOfDay = strategy.timeOfDay != null ? strategy.timeOfDay : "-";
			const holdingTimePattern = /\d+/;
			const holdingTimeMatch = holdingTimePattern.exec(strategy.exit);
			const holdingTimeHours = parseInt(holdingTimeMatch[0]);
			const holdingTime = `${holdingTimeHours}h`;
			const cells1 = [
				["Strategy", strategyName, false],
				["Feature 1", features[0], false],
				["Feature 2", features[1], false],
				["Options", optionsString, false],
				["Entry", timeOfDay, false],
				["Holding Time", holdingTime, false],
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

function showStrategyDetails(title, strategy) {
	const padding = 35;
	const width = 1152 + padding;
	const height = 1100 + padding;
	const left = 100;
	const top = 100;
	let linkHtml = "";
	const links = document.querySelectorAll("link");
	for (let i = 0; i < links.length; i++) {
		linkHtml += links[i].outerHTML + "\n";
	}
	const details = window.open("", "_blank", `width=${width},height=${height},left=${left},top=${top},resizable=yes`);
	details.document.write(`
		<!doctype html>
			<head>
				<title>${title}</title>
				${linkHtml}
			</head>
		</html>
	`);
	details.document.close();
	const container = createElement("div", details.document.body, "strategyDetails");
	const plotRow = createElement("div", container, "equityCurve");
	createElement("img", plotRow, {
		src: strategy.plot
	});
	const weekdayRow = createElement("div", container, "weekdayPlots");
	createElement("img", weekdayRow, {
		src: strategy.weekdayPlot
	});
	createElement("img", weekdayRow, {
		src: strategy.recentPlot
	});
}
addEventListener("DOMContentLoaded", event => {
	renderDataMiningUI();
});