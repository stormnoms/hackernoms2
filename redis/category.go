package main

import (
	"fmt"
	"math"
)

const denominator = 100

func getMod(x float64) float64 {
	remainder := math.Mod(x, 100.0)
	return remainder
}

/*
Given an integer it will tell you what category its in.
For example numbers between
  0 -  99 are in category 0
100 - 199 are in category 1
200 - 299 are in category 2
*/

func getCat(i int) (cat int) {
	x := float64(i)
	y := x / float64(denominator)
	z, _ := math.Modf(y)
	cat = int(z)
	return cat
}

func drive() {
	var y int
	for i := 0; i < 300; i++ {
		y = getCat(i)
		fmt.Println(i, y)
	}
}

func main() {
	drive()
}
