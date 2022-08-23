// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

const DEFAULT_ERROR_BUFFER = 100

// PIController
type PIController struct {
	inflight   *inflight
	proportion float64
	integral   float64
	lastSum    float64
}

// NewPIController
func NewPIController(proportion, integral float64) *PIController {
	return &PIController{
		inflight:   newInflight(DEFAULT_ERROR_BUFFER),
		lastSum:    0.0,
		proportion: proportion,
		integral:   integral,
	}
}

// AddError
func (p *PIController) AddError(err float64) float64 {
	//old := p.inflight.Add(err)
	p.lastSum = p.lastSum + err
	return p.proportion*err + p.integral*p.lastSum
}

type inflight struct {
	array []float64
	start int
	size  int
}

func newInflight(size int) *inflight {
	return &inflight{
		array: make([]float64, size),
		size:  size,
	}
}

func (f *inflight) Add(element float64) float64 {
	idx := f.index()
	old := f.array[idx]
	f.array[idx] = element
	f.start++
	return old
}

func (f *inflight) index() int {
	return f.start % f.size
}
