package sibylla

import (
	"encoding/json"
	"log"
	"path/filepath"
	"strings"

	"github.com/jchv/go-webview2"
)

const templateFilename = "template.html"
const stylesheet = "style.css"
const stylesheetPlaceholder = "STYLESHEET_PATH"
const jsonPlaceholder = "MODEL_JSON"
const scriptPathPlaceholder = "SCRIPT_PATH"

func runBrowser(title, script string, model any) {
	windowOptions := webview2.WindowOptions{
		Title: title,
		Width: 1280,
		Height: 960,
		IconId: 1,
		Center: true,
	}
	options := webview2.WebViewOptions{
		Debug: true,
		AutoFocus: true,
		WindowOptions: windowOptions,
	}
	w := webview2.NewWithOptions(options)
	if w == nil {
		log.Fatalln("Failed to load WebView")
	}
	defer w.Destroy()
	scriptPath := filepath.Join(configuration.WebPath, script)
	html := getTemplateHtml(scriptPath, model)
	htmlPath := filepath.Join(configuration.TempPath, templateFilename)
	writeFile(htmlPath, html)
	htmlURL := getFileURL(htmlPath)
	w.Navigate(htmlURL)
	w.Run()
}

func getTemplateHtml(scriptPath string, model any) string {
	templatePath := filepath.Join(configuration.WebPath, templateFilename)
	templateBytes := readFile(templatePath)
	templateString := string(templateBytes)
	jsonBytes, err := json.Marshal(model)
	if err != nil {
		log.Fatal("Failed to serialize validation model to JSON:", err)
	}
	jsonString := string(jsonBytes)
	stylesheetPath := filepath.Join(configuration.WebPath, stylesheet)
	scriptURL := getFileURL(scriptPath)
	html := templateString
	html = strings.Replace(html, stylesheetPlaceholder, stylesheetPath, 1)
	html = strings.Replace(html, jsonPlaceholder, jsonString, 1)
	html = strings.Replace(html, scriptPathPlaceholder, scriptURL, 1)
	return html
}

func getFileURL(path string) string {
	return "file:///" + path
}