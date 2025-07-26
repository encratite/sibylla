function renderArchiveUI() {
	const model = getModel();
	const topLevel = createElement("div", document.body, "container-archive");
	const dailyRecords = createElement("div", topLevel, "dailyRecords");
	createElement("img", dailyRecords, {
		src: model.plot
	});
	model.properties.forEach(property => {
		const container = createElement("div", topLevel, "property");
		const addMissingValueStyle = valueCell => {
			if (property.nilRatio >= 0.1) {
				valueCell.style.color = "#ff0000";
			}
		};
		const properties = [
			["Property", property.name],
			["Missing Values", getPercentage(property.nilRatio), addMissingValueStyle],
			["Minimum", roundValue(property.min)],
			["Maximum", roundValue(property.max)],
			["Mean", roundValue(property.mean)],
			["Standard Deviation", roundValue(property.stdDev)],
		];
		const table = createElement("table", container);
		properties.forEach(definition => {
			const description = definition[0];
			const value = definition[1];
			const handler = definition[2];
			const row = createElement("tr", table);
			const descriptionCell = createElement("td", row);
			descriptionCell.textContent = `${description}:`;
			const valueCell = createElement("td", row);
			valueCell.textContent = value;
			if (handler != null) {
				handler(valueCell);
			}
		});
		createElement("img", container, {
			src: property.plot
		});
	});
}

function roundValue(value) {
	if (Number.isInteger(value)) {
		return value.toString();
	} else {
		const output = value.toFixed(3).toString();
		if (output === "0.000") {
			return "0";
		} else {
			return output;
		}
	}
}

addEventListener("DOMContentLoaded", event => {
	renderArchiveUI();
});