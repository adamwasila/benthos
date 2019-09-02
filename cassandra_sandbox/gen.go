package main

import (
	"fmt"
)

func main() {
	for i:=0; i<1024*1024*1024/128; i++ {
		fmt.Printf("%.127d\n", i)
		if i % (1024*1024) == (1024*1024)-1 {
			fmt.Println()
		}
	}
}
