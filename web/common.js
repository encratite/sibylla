function getModel() {
	const jsonScript = document.getElementById("model");
	const model = JSON.parse(jsonScript.textContent);
	return model;
}

function createElement(tagName, container, properties) {
	const element = document.createElement(tagName);
	if (container != null) {
		container.appendChild(element);
	}
	if (properties != null) {
		if (typeof properties === "string") {
			element.className = properties;
		} else {
			for (const key in properties) {
				element[key] = properties[key];
			}
		}
	}
	return element;
}

function getPercentage(value) {
	const percentage = (value * 100).toFixed(2);
	return `${percentage}%`;
}

function formatMoney(amount) {
	const options = {
		style: "currency",
		currency: "USD",
	};
	const format = new Intl.NumberFormat("en-US", options);
	const output = format.format(amount);
	return output;
}