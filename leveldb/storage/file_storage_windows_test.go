package storage

import "testing"

// Windows permits readers beside one writer; a second writer must fail until the first writer closes.
func TestFileStorage_WindowsOpenFileSharing(t *testing.T) {
	for _, firstReadOnly := range []bool{false, true} {
		for _, secondReadOnly := range []bool{false, true} {
			for _, closeFirst := range []bool{false, true} {
				name := "writer"
				if firstReadOnly {
					name = "reader"
				}
				name += "_then_"
				if secondReadOnly {
					name += "reader"
				} else {
					name += "writer"
				}
				if closeFirst {
					name += "_close_first"
				} else {
					name += "_close_second"
				}
				t.Run(name, func(t *testing.T) {
					dir := t.TempDir()
					seed, err := OpenFile(dir, false)
					if err != nil {
						t.Fatal(err)
					}
					if err := seed.Close(); err != nil {
						t.Fatal(err)
					}
					first, err := OpenFile(dir, firstReadOnly)
					if err != nil {
						t.Fatal(err)
					}
					defer first.Close()
					second, err := OpenFile(dir, secondReadOnly)
					if !firstReadOnly && !secondReadOnly {
						if err == nil {
							second.Close()
							t.Fatal("second writer opened beside first writer")
						}
						if err := first.Close(); err != nil {
							t.Fatal(err)
						}
						replacement, err := OpenFile(dir, false)
						if err != nil {
							t.Fatalf("writer after first writer closed: %v", err)
						}
						if err := replacement.Close(); err != nil {
							t.Fatal(err)
						}
						return
					}
					if err != nil {
						t.Fatalf("allowed OpenFile combination: %v", err)
					}
					defer second.Close()
					third, err := OpenFile(dir, false)
					if !firstReadOnly || !secondReadOnly {
						if err == nil {
							third.Close()
							t.Fatal("third writer opened while writer and reader both remained open")
						}
					} else {
						if err != nil {
							t.Fatalf("writer beside two readers: %v", err)
						}
						if err := third.Close(); err != nil {
							t.Fatal(err)
						}
					}
					survivor := first
					if closeFirst {
						if err := first.Close(); err != nil {
							t.Fatal(err)
						}
						survivor = second
					} else {
						if err := second.Close(); err != nil {
							t.Fatal(err)
						}
					}
					survivorReadOnly := firstReadOnly
					if closeFirst {
						survivorReadOnly = secondReadOnly
					}
					writer, err := OpenFile(dir, false)
					if survivorReadOnly {
						if err != nil {
							t.Fatalf("writer beside surviving reader: %v", err)
						}
						if err := writer.Close(); err != nil {
							t.Fatal(err)
						}
					} else if err == nil {
						writer.Close()
						t.Fatal("closing reader released surviving writer's lock")
					}
					if err := survivor.Close(); err != nil {
						t.Fatal(err)
					}
					writer, err = OpenFile(dir, false)
					if err != nil {
						t.Fatalf("writer after both closed: %v", err)
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}
