package main

// func TestExpr(t *testing.T) {
// 	// Example list of expressions
// 	exprs := []string{
// 		"int5 == 5",
// 		"id == ''",
// 		"id == '' AND state.wtf == 'hello'",
// 	}

// 	evalContext := WorkflowExpr{
// 		State: []byte(`{"wtf":"hello"}`),
// 	}
// 	for _, expression := range exprs {
// 		// Same ast can be re-used safely concurrently
// 		exprAst := expr.MustParse(expression)
// 		// Evaluate AST in the vm
// 		val, ok := vm.Eval(evalContext, exprAst)
// 		if !ok {
// 			log.Printf("Eval not ok: %v", val)
// 		}
// 		log.Printf("Output: %v expr:  %s", val, expression)
// 	}
// }
