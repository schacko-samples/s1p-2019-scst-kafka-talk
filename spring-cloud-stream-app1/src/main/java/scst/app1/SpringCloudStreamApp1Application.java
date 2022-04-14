package scst.app1;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.github.javafaker.Book;
import com.github.javafaker.Faker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringCloudStreamApp1Application {

	public static void main(String[] args) {
		SpringApplication.run(SpringCloudStreamApp1Application.class, args);
	}

	@Bean
	public Supplier<Foo> supplierFoo() {
		return () -> {
			Faker faker = Faker.instance();
			final Book book = faker.book();
			Foo foo = new Foo();
			foo.setTitle(book.title());
			foo.setAuthor(book.author());
			foo.setGenre(book.genre());
			foo.setPublisher(book.publisher());
			return foo;
		};
	}

	@Bean
	public Consumer<Foo> consumerFoo() {
		return System.out::println;
	}

	@Bean
	public Function<Foo, String> functionFoo()  {
		return foo -> foo.toString().toUpperCase();
	}

	@Bean
	public Consumer<String> consumerText() {
		return System.out::println;
	}

	static class Foo {

		private String title;
		private String author;
		private String genre;
		private String publisher;

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public String getAuthor() {
			return author;
		}

		public void setAuthor(String author) {
			this.author = author;
		}

		public String getGenre() {
			return genre;
		}

		public void setGenre(String genre) {
			this.genre = genre;
		}

		public String getPublisher() {
			return publisher;
		}

		public void setPublisher(String publisher) {
			this.publisher = publisher;
		}

		@Override
		public String toString() {
			return "Foo{" +
					"title='" + title + '\'' +
					", author='" + author + '\'' +
					", genre='" + genre + '\'' +
					", publisher='" + publisher + '\'' +
					'}';
		}
	}

}
