package main

import (
	"net/http"

	"github.com/bitly/go-simplejson"
)

type DruidResponse struct {
	timestamp int64
	result    string `json:"type"`
}

type TargetResponseDTO struct {
	Target     string           `json:"target,omitempty"`
	DataPoints TimeSeriesPoints `json:"datapoints,omitempty"`
	Columns    []TableColumn    `json:"columns,omitempty"`
	Rows       []RowValues      `json:"values,omitempty"`
}

type TimePoint [2]float64
type TimeSeriesPoints []TimePoint

type TableColumn struct {
	Text string `json:"text"`
	Type string `json:"type"`
}

type RowValues []interface{}

type remoteDatasourceRequest struct {
	queryType string
	req       *http.Request
	queries   []*simplejson.Json
}
