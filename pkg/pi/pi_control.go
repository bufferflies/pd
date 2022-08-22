package pi

// PIController
type PIController struct {
	lastErr    float64
	proportion float64
	integral   float64
	deltaValue float64
}

// NewPIController
func NewPIController(proportion, integral float64) *PIController {
	return &PIController{
		lastErr:    0.0,
		deltaValue: 0.0,
		proportion: proportion,
		integral:   integral,
	}
}

// AddError
func (p *PIController) AddError(err float64) float64 {
	val := p.proportion*err - p.integral*p.lastErr
	p.lastErr = err
	return val
}
