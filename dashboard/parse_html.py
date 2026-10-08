from html.parser import HTMLParser

class MyHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags = []

    def handle_starttag(self, tag, attrs):
        if tag not in ['meta', 'link', 'img', 'br', 'hr', 'input']:
            self.tags.append(tag)

    def handle_endtag(self, tag):
        if tag not in ['meta', 'link', 'img', 'br', 'hr', 'input']:
            if self.tags and self.tags[-1] == tag:
                self.tags.pop()
            else:
                print(f"Error: expected </{self.tags[-1] if self.tags else 'none'}> but got </{tag}>")
                if tag in self.tags:
                    while self.tags[-1] != tag:
                        self.tags.pop()
                    self.tags.pop()

parser = MyHTMLParser()
with open('index.html', 'r') as f:
    parser.feed(f.read())
print("Unclosed tags remaining:", parser.tags)
