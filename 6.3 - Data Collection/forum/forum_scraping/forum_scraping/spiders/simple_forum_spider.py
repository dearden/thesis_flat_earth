import scrapy
import re
import os


class SimpleForumScraper(scrapy.Spider):
    # A unique (within the project) name for the spider.
    name = "simple_forum_scraper"

    # These are some regular expressions to identify urls that point to boards and topics.
    # Do not worry too much about this, you'll cover regular expressions in a future lab.
    reg_topic = re.compile(r'.*\/?index\.php\?.*topic=(\d+\.?\d*)')
    reg_board = re.compile(r'.*\/?index\.php\?.*board=(\d+\.?\d*)')
    reg_users = re.compile(r'.*u=(\d+)')

    def __init__(self, folder="forum", domain=None, *a, **kw):
        super(SimpleForumScraper, self).__init__(*a, **kw)
        self.folder = folder
        self.allowed_domains = [domain]
        self.start_urls = ["http://" + domain]
        self.check_folder(self.folder)
        self.check_folder("{0}/boards".format(self.folder))
        self.check_folder("{0}/topics".format(self.folder))
        self.check_folder("{0}/users".format(self.folder))

        self.users_added = []
        self.counter = 0

    # This method tells the scraper how to start. It must return an iterable of Requests.
    def start_requests(self):
        # The URLS the scraper will go through
        urls = ["http://forum.tfes.org"]      # INSERT YOUR BOARD URL HERE
        for url in urls:
            # yield the request. define the callback function.
            # The callback function will be called to handle the response of this request.
            yield scrapy.Request(url=url, callback=self.forum_parse)

    # Method that parses the top level forum page, with the boards on it.
    def forum_parse(self, response):
        # loops through all links on the page
        # specifically, we are looking for "a" html elements, which contain links, and getting the "href" attributes.
        for link in response.css('a::attr(href)').getall():
            if self.reg_board.fullmatch(link):         # Makes a request for the url if it's a board.
                yield response.follow(link, callback=self.board_parse)

    # The method that handles the response (instance of TextResponse)
    # Usually this method will take information from the current page and then create new Requests.
    def board_parse(self, response):
        # Gets the board number from the url with a regular expression.
        board_num = self.reg_board.match(response.url).group(1)

        # loops through all links on the page
        # specifically, we are looking for "a" html elements, which contain links, and getting the "href" attributes.
        for link in response.css('a::attr(href)').getall():
            if self.reg_topic.fullmatch(link):         # Makes a request for the url if it's a topic.
                yield response.follow(link, callback=self.topic_parse)

        # Dump the board to a file.
        filename = '{0}/boards/{1}-board={2}.html'.format(self.folder, self.counter, board_num)
        self.counter += 1
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file {}'.format(filename))

        # Find the url for the next page (link with the text "Next")
        next_page = response.xpath('//a[contains(text(), "Next")]/@href').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.board_parse)

    # Parses topic pages, dumps the contents into an html file.
    def topic_parse(self, response):
        # Gets the topic number.
        topic_num = self.reg_topic.match(response.url).group(1)

        # Dumps the page to a file.
        filename = '{0}/topics/{1}-topic={2}.html'.format(self.folder, self.counter, topic_num)
        self.counter += 1
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file {}'.format(filename))

        # loops through all links on the page
        # specifically, we are looking for "a" html elements, which contain links, and getting the "href" attributes.
        # Only want to follow the ones that correspond to users that we've not already added.
        for link in response.css('a::attr(href)').getall():
            user_match = self.reg_users.fullmatch(link)        # Makes a request for the url if it's a user.
            if user_match:
                curr_user = user_match.group(1)
                if curr_user not in self.users_added:
                    self.users_added += curr_user
                    yield response.follow(link, callback=self.user_parse)


        # Find the url for the next page (link with the text "Next")
        next_page = response.xpath('//a[contains(text(), "Next")]/@href').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.topic_parse)

    # method for parsing a user page
    def user_parse(self, response):
        # Gets the topic number.
        curr_user = self.reg_users.match(response.url).group(1)

        print("Parsing User - {}".format(curr_user))
        filename = '{0}/users/{1}-u={2}.html'.format(self.folder, self.counter, curr_user)
        self.counter += 1
        with open(filename, 'wb') as f:
            f.write(response.body)

    # This just makes a folder if you don't have one.
    def check_folder(self, folder):
        if not os.path.exists(folder):
            os.mkdir(folder)
