package main

import (
	"fmt"
	"os"
	"time"
	"github.com/briandowns/spinner"
)

func processBar2(){
	s := spinner.New(spinner.CharSets[9], 100*time.Millisecond)  // Build our new spinner
	s.Start()                                                    // Start the spinner
	time.Sleep(4 * time.Second)                                  // Run for some time to simulate work
	s.Stop()
}

func processBar1(){
	for i :=0;i!=10;i=i+1{
		fmt.Fprintf(os.Stdout,"result is %d\r",i)
		time.Sleep(time.Second*1)
	}
	fmt.Println("Over")
}


func main(){
	//processBar1()
	processBar2()
}