package sibylla

import (
	"encoding/json"
	"log"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/jchv/go-webview2"
	"github.com/lxn/win"
)

const templateFileName = "index.html"
const stylesheetFileName = "style.css"
const commonScriptFileName = "common.js"
const stylesheetPlaceholder = "STYLESHEET_PATH"
const jsonPlaceholder = "MODEL_JSON"
const commonPathPlaceholder = "COMMON_PATH"
const scriptPathPlaceholder = "SCRIPT_PATH"

func runBrowser(title, script string, model any, large bool) {
	var width, height uint
	if large {
		width = 1440
		height = 1080
	} else {
		width = 1280
		height = 960
	}
	windowOptions := webview2.WindowOptions{
		Title: title,
		Width: width,
		Height: height,
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
	iconPath := configuration.IconPath
	iconPathUTF16Ptr, err := syscall.UTF16PtrFromString(iconPath)
	if err != nil {
		log.Fatal("Failed to convert string:", err)
	}
	hIcon := win.HICON(win.LoadImage(
		0,
		iconPathUTF16Ptr,
		win.IMAGE_ICON,
		0,
		0,
		win.LR_LOADFROMFILE | win.LR_DEFAULTSIZE,
	))
	if hIcon == 0 {
		log.Fatalf("Failed to load icon from %s", iconPath)
	}
	hWnd := w.Window()
	win.SendMessage(win.HWND(hWnd), win.WM_SETICON, 0, uintptr(hIcon))
	scriptPath := filepath.Join(configuration.WebPath, script)
	html := getTemplateHtml(scriptPath, model)
	htmlPath := filepath.Join(configuration.TempPath, templateFileName)
	writeFile(htmlPath, html)
	htmlURL := getFileURL(htmlPath)
	w.Navigate(htmlURL)
	w.Run()
}

func getTemplateHtml(scriptPath string, model any) string {
	templatePath := filepath.Join(configuration.WebPath, templateFileName)
	templateBytes := readFile(templatePath)
	templateString := string(templateBytes)
	jsonBytes, err := json.Marshal(model)
	if err != nil {
		log.Fatal("Failed to serialize validation model to JSON:", err)
	}
	jsonString := string(jsonBytes)
	commonPath := filepath.Join(configuration.WebPath, commonScriptFileName)
	stylesheetPath := filepath.Join(configuration.WebPath, stylesheetFileName)
	commonURL := getFileURL(commonPath)
	scriptURL := getFileURL(scriptPath)
	html := templateString
	html = strings.Replace(html, stylesheetPlaceholder, stylesheetPath, 1)
	html = strings.Replace(html, jsonPlaceholder, jsonString, 1)
	html = strings.Replace(html, commonPathPlaceholder, commonURL, 1)
	html = strings.Replace(html, scriptPathPlaceholder, scriptURL, 1)
	return html
}

func getFileURL(path string) string {
	return "file:///" + path
}