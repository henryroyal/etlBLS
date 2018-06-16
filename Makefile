NAME=etl

all: $(NAME)

$(NAME): deps
	go build -o $(NAME) main.go

deps:
	dep ensure

clean:
	rm $(NAME)