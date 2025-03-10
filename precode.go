package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Generator генерирует последовательность чисел 1,2,3 и т.д. и
// отправляет их в канал ch. При этом после записи в канал для каждого числа
// вызывается функция fn. Она служит для подсчёта количества и суммы
// сгенерированных чисел.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	// 1. Функция Generator
	// ...
	// N(0) = 1;
	// N(i) = N(i-1) + 1
	fmt.Println("Функция Generator")
	var num int64
	num = 1
	for {

		select {
		case <-ctx.Done():
			fmt.Println("ctx.Done")
			close(ch)
			break
		default:
			ch <- num
			fn(num)
			//fmt.Println("inputCount= ", inputCount)
			num++
			continue
		}
	}
	fmt.Println("End of Generator")
}

// Worker читает число из канала in и пишет его в канал out.
func Worker(in <-chan int64, out chan<- int64) {
	// 2. Функция Worker
	// ...
	for {
		// получаем числа из канала
		v, ok := <-in
		if !ok {
			close(out)
			break
		}
		out <- v
		fmt.Println("Worker v = ", v)
		time.Sleep(1 * time.Millisecond)
	}
	//return

}

func main() {
	chIn := make(chan int64)

	// 3. Создание контекста
	// ...
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// для проверки будем считать количество и сумму отправленных чисел
	var inputSum int64   // сумма сгенерированных чисел
	var inputCount int64 // количество сгенерированных чисел

	// генерируем числа, считая параллельно их количество и сумму
	go Generator(ctx, chIn, func(i int64) {

		//inputSum += i
		//inputCount++
		atomic.AddInt64(&inputSum, i)
		atomic.AddInt64(&inputCount, 1)
		fmt.Println("inputSum=", inputSum, " inputCount=", inputCount)
	})

	fmt.Println("After Generator")

	const NumOut = 5 // количество обрабатывающих горутин и каналов
	// outs — слайс каналов, куда будут записываться числа из chIn
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		// создаём каналы и для каждого из них вызываем горутину Worker
		outs[i] = make(chan int64)
		go Worker(chIn, outs[i])
	}
	fmt.Println("After Worker")
	// amounts — слайс, в который собирается статистика по горутинам
	amounts := make([]int64, NumOut)
	// chOut — канал, в который будут отправляться числа из горутин `outs[i]`
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup

	// 4. Собираем числа из каналов outs
	// ...
	var i int64
	for i = 0; i < NumOut; i++ {
		wg.Add(1)
		go func(in <-chan int64, i int64) {
			wg.Done()
			for a := range in {
				chOut <- a
				amounts[i]++
			}
		}(outs[i], int64(i))

	}
	//fmt.Println("After 4")
	go func() {
		// ждём завершения работы всех горутин для outs
		wg.Wait()
		// закрываем результирующий канал
		close(chOut)
	}()

	var count int64 // количество чисел результирующего канала
	var sum int64   // сумма чисел результирующего канала

	// 5. Читаем числа из результирующего канала
	// ...
	count = 0
	sum = 0
	for v := range chOut {
		count++
		sum = sum + v
	}

	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	// проверка результатов
	if inputSum != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount, count)
	}
	for _, v := range amounts {
		inputCount -= v
	}
	if inputCount != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}
