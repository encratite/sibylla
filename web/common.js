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