function renderArchiveUI() {
	const jsonScript = document.getElementById("model");
	const model = JSON.parse(jsonScript.textContent);
	const topLevel = createElement("div", document.body, {
		className: "container"
	});
	const dailyRecords = createElement("div", topLevel, {
		className: "dailyRecords"
	});
	createElement("img", dailyRecords, {
		src: model.plot
	});
	model.features.forEach(feature => {
		const container = createElement("div", topLevel, {
			className: "feature"
		});
		const addMissingValueStyle = valueCell => {
			if (feature.nilRatio >= 0.1) {
				valueCell.style.color = "#ff0000";
			}
		};
		const properties = [
			["Property", feature.name],
			["Missing Values", getPercentage(feature.nilRatio), addMissingValueStyle],
			["Minimum", roundValue(feature.min)],
			["Maximum", roundValue(feature.max)],
			["Mean", roundValue(feature.mean)],
			["Standard Deviation", roundValue(feature.stdDev)],
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
			src: feature.plot
		});
	});
}

function createElement(tagName, container, properties) {
	const element = document.createElement(tagName);
	if (container != null) {
		container.appendChild(element);
	}
	if (properties != null) {
		for (const key in properties) {
			element[key] = properties[key];
		}
	}
	return element;
}

function getPercentage(value) {
	const percentage = (value * 100).toFixed(2);
	return `${percentage}%`;
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