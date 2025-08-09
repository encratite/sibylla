function renderDataMiningUI() {
	const model = getModel();
	const container = createElement("div", document.body, {
		className: "containerDataMine"
	});
	if (model.features !== null) {
		const featuresContainer = createElement("div", container);
		renderFeatures(model.features, featuresContainer);
	}
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
			let strategyName = `${asset.symbol} Strategy #${index + 1}`;
			const equityCurve = createElement("img", null, {
				src: strategy.plot,
				className: "equityCurve",
				onclick: () => showStrategyDetails(strategyName, strategy),
			});
			const truncateCondition = limit => {
				const precision = 100;
				return Math.round(precision * limit) / precision;
			};
			const features = strategy.features.map(feature => {
				const min = truncateCondition(feature.min);
				const max = truncateCondition(feature.max);
				return `${feature.symbol}.${feature.name} (${min}, ${max})`;
			});
			const side = strategy.side === 0 ? "Long" : "Short";
			let options = [];
			if (strategy.optimizeWeekdays === true) {
				options.push("Weekday optimization");
			}
			if (strategy.stopLoss !== null) {
				options.push(`Stop-loss at ${getPercentage(strategy.stopLoss, 1)}`);
			}
			if (options.length === 0) {
				options.push("-");
			}
			const optionsString = options.join(", ");
			const timeOfDay = strategy.timeOfDay != null ? strategy.timeOfDay : "-";
			const holdingTimePattern = /\d+/;
			const holdingTimeMatch = holdingTimePattern.exec(strategy.exit);
			const holdingTimeHours = parseInt(holdingTimeMatch[0]);
			const holdingTime = `${holdingTimeHours}h`;
			let cells1;
			if (model.seasonalityMode === true) {
				cells1 = [
					["Side", side, false],
					["Weekday", getWeekdayString(strategy.weekday), false],
					["Entry", timeOfDay, false],
					["Holding Time", holdingTime, false],
					["", "", false],
					["", "", false],
				];
			} else {
				const feature1 = features[0];
				const feature2 = features[0] !== features[1] ? features[1] : "-";
				cells1 = [
					["Feature 1", feature1, false],
					["Feature 2", feature2, false],
					["Side", side, false],
					["Entry", timeOfDay, false],
					["Holding Time", holdingTime, false],
					["Options", optionsString, false],
				];
			}
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
			const firstRow = createElement("tr", table);
			createElement("th", firstRow, {
				textContent: strategyName,
				colSpan: 4
			});
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

function getWeekdayString(weekday) {
	const weekdays = [
		"Monday",
		"Tuesday",
		"Wednesday",
		"Thursday",
		"Friday"
	];
	const index = weekday - 1;
	if (index < 0 || index >= weekdays.length) {
		throw new Error(`Invalid weekday: ${weekday}`);
	}
	return weekdays[index];
}

function renderFeatures(features, container) {
	const header = createElement("h1", container);
	header.textContent = "Features";
	const innerContainer = createElement("div", container, "features");
	renderFeatureHeatmap(features, innerContainer);
	const featureSlots = features.features[0].frequencies.length;
	for (let featureIndex = 0; featureIndex < featureSlots; featureIndex++) {
		const table = createElement("table", innerContainer);
		const headerRow = createElement("tr", table);
		const headers = [
			`Feature ${featureIndex + 1}`,
			"Frequency",
		];
		headers.forEach(header => {
			const cell = createElement("th", headerRow);
			cell.textContent = header;
		});
		features.features.sort((a, b) => {
			return b.frequencies[featureIndex] - a.frequencies[featureIndex];
		});
		features.features.forEach(f => {
			const frequency = f.frequencies[featureIndex];
			const row = createElement("tr", table);
			const cell1 = createElement("td", row);
			cell1.textContent = f.name;
			const cell2 = createElement("td", row);
			cell2.textContent = getPercentage(frequency, 1);
		});
	}
}

function renderFeatureHeatmap(features, container) {
	const names = features.features.map(x => x.name);
	const xValues = names;
	const yValues = names;
	const zValues = features.combinations;
	const featureCount = names.length;
	const textData = [];
	for (let x = 0; x < featureCount; x++) {
		const row = [];
		for (let y = 0; y < featureCount; y++) {
			const value = features.combinations[x][y];
			const percentage = getPercentage(value, 1);
			row.push(percentage)
		}
		textData.push(row);
	}
	const data = [{
		x: xValues,
		y: yValues,
		z: zValues,
		type: "heatmap",
		colorscale: "Viridis",
		text: textData,
		texttemplate: "%{text}",
		hoverinfo: "skip",
		showscale: true,
		colorbar: {
			tickformat: ".0%"
		},
	}];
	const layout = {
		title: {
			text: "Frequency of Combinations",
			font: {
				size: 18
			},
		},
		font: {
			family: "Roboto",
			size: 14,
		},
		width: 700,
		margin: {
			t: 40,
			b: 100,
			l: 120,
			r: 50
		}
	};
	const config = {
		displayModeBar: false
	};
	const id = "featureHeatmap";
	createElement("div", container, {
		id: id
	});
	Plotly.newPlot(id, data, layout, config);
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