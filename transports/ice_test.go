package transports

import (
	"strings"
	"testing"
)

func TestValidateICEDescriptionLimits(t *testing.T) {
	description := ICEDescription{
		UsernameFragment: "user",
		Password:         "password",
		Candidates:       []string{"candidate-a", "candidate-b"},
	}
	if err := ValidateICEDescriptionLimits(description, 2, 64); err != nil {
		t.Fatalf("valid ICE description rejected: %v", err)
	}
	if err := ValidateICEDescriptionLimits(description, 1, 64); err == nil {
		t.Fatal("candidate count limit was not enforced")
	}
	description.Candidates = []string{strings.Repeat("x", 53)}
	if err := ValidateICEDescriptionLimits(description, 2, 64); err == nil {
		t.Fatal("ICE byte limit was not enforced")
	}
}
