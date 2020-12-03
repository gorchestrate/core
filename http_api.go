package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

type HTTPAPI struct {
	srv *Server
}

func (api HTTPAPI) Router() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/findWorkflows", api.FindWorkflowsHandler)
	return r
}

func jsonErr(w http.ResponseWriter, code int, err error, reasons ...string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(struct {
		Error  string
		Reason string
	}{
		Error:  err.Error(),
		Reason: strings.Join(reasons, ","),
	})
}

func (api HTTPAPI) FindWorkflowsHandler(w http.ResponseWriter, r *http.Request) {
	var req FindWorkflowsReq
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		jsonErr(w, 400, err, "json decode err")
		return
	}
	resp, err := api.srv.FindWorkflows(r.Context(), &req)
	if err != nil {
		jsonErr(w, 400, err, "find workflows err")
		return
	}
	if r.URL.Query().Get("pretty") == "true" {
		e := json.NewEncoder(w)
		e.SetIndent("", " ")
		_ = e.Encode(resp)
	} else {
		_ = json.NewEncoder(w).Encode(resp)
	}
}
