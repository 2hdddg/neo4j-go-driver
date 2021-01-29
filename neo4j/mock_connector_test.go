/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

// Code generated by MockGen. DO NOT EDIT.
// Source: neo4j-go-connector/neo4j (interfaces: Connector)

// Package connector-mocks is a generated GoMock package.
package neo4j

import (
	reflect "reflect"

	"github.com/neo4j-drivers/gobolt"

	gomock "github.com/golang/mock/gomock"
)

// MockConnector is a mock of Connector interface
type MockConnector struct {
	ctrl     *gomock.Controller
	recorder *MockConnectorMockRecorder
}

// MockConnectorMockRecorder is the mock recorder for MockConnector
type MockConnectorMockRecorder struct {
	mock *MockConnector
}

// NewMockConnector creates a new mock instance
func NewMockConnector(ctrl *gomock.Controller) *MockConnector {
	mock := &MockConnector{ctrl: ctrl}
	mock.recorder = &MockConnectorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockConnector) EXPECT() *MockConnectorMockRecorder {
	return m.recorder
}

// Close connector-mocks base method
func (m *MockConnector) Close() error {
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close
func (mr *MockConnectorMockRecorder) Close() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockConnector)(nil).Close))
}

// GetPool connector-mocks base method
func (m *MockConnector) Acquire(arg0 gobolt.AccessMode) (gobolt.Connection, error) {
	ret := m.ctrl.Call(m, "Acquire", arg0)
	ret0, _ := ret[0].(gobolt.Connection)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPool indicates an expected call of GetPool
func (mr *MockConnectorMockRecorder) Acquire(arg0 gobolt.AccessMode) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Acquire", reflect.TypeOf((*MockConnector)(nil).Acquire), arg0)
}
