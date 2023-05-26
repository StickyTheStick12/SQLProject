import os
from datetime import datetime
import mysql.connector
import random

cnx = mysql.connector.connect(host='localhost',
                              user='root',
                              password='root')
cursor = cnx.cursor(dictionary=True)
experimental = False

def AddPizzaRecipe(n: int, toppingCSV: str):
    toppingCSV = toppingCSV.split(",")
    for num in toppingCSV:
        cursor.execute("insert into PizzaRecipes values({0}, {1})".format(n, num))


def Experimental():
    global experimental
    experimental = True
    cursor.execute("""alter table RunnersOrders add Price float""")

    cursor.execute("""update RunnersOrders RO inner join (select OrderId, PizzaId from CustomersOrders) CO
                    on RO.OrderId = CO.OrderId
                    set RO.Price = case when CO.PizzaId = 1 then 12 else 10 END""")

    cursor.execute("""drop trigger if exists IncreaseOrderPrice""")
    cursor.execute("""
                    create trigger IncreaseOrderPrice
                    before insert on RunnersOrders for each row
                    begin
                        set new.Price = new.Price + 5 + new.Distance*2;
                    end""")

    cursor.execute("""drop trigger if exists setPrice""")
    cursor.execute("""
                    create trigger SetPrice 
                    before insert on RunnersOrders for each row precedes IncreaseOrderPrice 
                    BEGIN
                        select PizzaId into @pId from CustomersOrders where OrderId = new.OrderId;
                        
                        set new.Price = case when @pId = 1 then 12 else 10 END;
                    END""")

    cursor.execute("""drop procedure if exists lowerPrice""")
    cursor.execute("""
                    create procedure lowerPrice(choice int, lowerPrice int, startFee int, startDate date, endDate date)
                    deterministic
                    begin
                        # choice 1 startFee is free
                        if choice = 1 then
                            update RunnersOrders
                            set Price = Price-startFee;
                        elseif choice = 2 then # choice 2 lower the prciePerKm by % 
                            update RunnersOrders
                            set Price = Price-Distance*(lowerPrice/100);
                        else #choice 3 lower the price for the pizza
                            update RunnersOrders
                            set Price = Price - 5;
                        end if;
                    end""")


def Start():
    # cursor.execute("CREATE DATABASE Pizzapp")
    cursor.execute("USE Pizzapp")
    cursor.execute("drop table RunnersOrders")

    cursor.execute("""drop table if exists Deliverymen""")
    cursor.execute("""
                CREATE TABLE Deliverymen(
                RunnerId int,
                DeliveryName text,
                RegistrationDate date,
                Rating float,
                primary key(RunnerId))""")

    cursor.execute("""insert into Deliverymen values(1, 'Lars Andersson', '2023-01-01', null)""")
    cursor.execute("""insert into Deliverymen values(2, 'Anna Nilsson', '2023-02-12', null)""")
    cursor.execute("""insert into Deliverymen values(3, 'Anders Johansson', '2023-02-13', null)""")
    cursor.execute("""insert into Deliverymen values(4, 'Maria Karlsson', '2023-03-03', null)""")

    cursor.execute("""drop table if exists CustomersOrders""")
    cursor.execute("""create table CustomersOrders(
                OrderId int,
                CustomerId int,
                PizzaId int,
                Exclusions varchar(4),
                Extras varchar(4),
                OrderTime datetime,
                primary key(OrderId))""")

    cursor.execute("""insert into CustomersOrders values(1, 101, 1, '', '', '2023-01-01 18:05:02')""")
    cursor.execute("""insert into CustomersOrders values(2, 101, 1, '', '', '2023-01-01 19:00:52')""")
    cursor.execute("""insert into CustomersOrders values(3, 102, 1, '', '', '2023-01-02 23:51:23')""")
    cursor.execute("""insert into CustomersOrders values(4, 103, 1, '4', '', '2023-01-04 13:23:46')""")
    cursor.execute("""insert into CustomersOrders values(5, 104, 1, '', '1', '2023-01-08 21:00:29')""")
    cursor.execute("""insert into CustomersOrders values(6, 101, 2, '', '', '2023-01-08 21:03:13')""")
    cursor.execute("""insert into CustomersOrders values(7, 105, 2, '', '1', '2023-01-08 21:20:29')""")
    cursor.execute("""insert into CustomersOrders values(8, 102, 1, '', '', '2023-01-09 23:54:33')""")
    cursor.execute("""insert into CustomersOrders values(9, 103, 1, '', '1, 5', '2023-01-10 11:22:59')""")
    cursor.execute("""insert into CustomersOrders values(10, 104, 1, '', '', '2023-01-11 18:34:49')""")
    cursor.execute("""insert into CustomersOrders values(11, 104, 1, '2, 6', '1, 4', '2023-01-11 18:34:49')""")

    cursor.execute("""drop table if exists RunnersOrders""")
    cursor.execute("""create table RunnersOrders(
                OrderId int,
                RunnerId int not null,
                PickupTime datetime,
                Distance float,
                Duration int,
                Cancellation varchar(30),
                Rating int,
                primary key(OrderId),
                foreign key(RunnerId) references Deliverymen(RunnerId))""")

    cursor.execute("""drop trigger if exists UpdateRatingAfterInsert""")
    cursor.execute("""
                    create trigger UpdateRatingAfterInsert
                    after insert 
                    on RunnersOrders for each ROW
                    begin
                        update Deliverymen D join (select RunnerId, avg(Rating) as aRating from RunnersOrders group by RunnerId) RO
                        on RO.RunnerId = D.RunnerId
                        set D.Rating = aRating
                        where D.RunnerId = new.RunnerId;
                    end""")

    cursor.execute("""insert into RunnersOrders values(1, 1, '2023-01-01 18:15:34', 20, 32, null, 3)""")
    cursor.execute("""insert into RunnersOrders values(2, 1, '2023-01-01 19:10:54', 20, 27, null, 4)""")
    cursor.execute("""insert into RunnersOrders values(3, 1, '2023-01-03 00:12:37', 13.4, 20, null, 1)""")
    cursor.execute("""insert into RunnersOrders values(4, 2, '2023-01-04 13:53:03', 23.4, 40, null, 5)""")
    cursor.execute("""insert into RunnersOrders values(5, 3, '2023-01-08 21:10:57', 10, 15, null, 1)""")
    cursor.execute(
        """insert into RunnersOrders values(6, 3, '2023-01-08 21:29:22', 1, 90, 'Restaurant Cancellation', null)""")
    cursor.execute("""insert into RunnersOrders values(7, 2, '2023-01-08 21:30:45', 25, 25, null, 3)""")
    cursor.execute("""insert into RunnersOrders values(8, 2, '2023-01-10 00:15:02', 23.4, 15, null, 4)""")
    cursor.execute(
        """insert into RunnersOrders values(9, 2, '2023-01-10 00:15:02', 0, 0, 'Customer Cancellation', null)""")
    cursor.execute("""insert into RunnersOrders values(10, 1, '2023-01-11 18:50:20', 10, 10, null, 5)""")

    cursor.execute("""drop table if exists PizzaNames""")
    cursor.execute("""create table PizzaNames(
                    PizzaId int,
                    PizzaName text,
                    primary key(PizzaId))""")

    cursor.execute("""insert into PizzaNames values(1, 'Meatlovers')""")
    cursor.execute("""insert into PizzaNames values(2, 'Vegetarian')""")

    cursor.execute("""drop table if exists PizzaRecipes""")
    cursor.execute("""create table PizzaRecipes(
                    PizzaId int,
                    ToppingID int,
                    primary key(PizzaId, ToppingID))""")

    AddPizzaRecipe(1, "1,2,3,4,5,6,7,8,10")
    AddPizzaRecipe(2, "4,6,7,9,11,12")

    cursor.execute("""drop table if exists PizzaToppings""")
    cursor.execute("""create table PizzaToppings(
                    ToppingId int,
                    ToppingName text,
                    primary key(ToppingId))""")

    cursor.execute("""insert into PizzaToppings values(1, 'Bacon')""")
    cursor.execute("""insert into PizzaToppings values(2, 'BBQ Sauce')""")
    cursor.execute("""insert into PizzaToppings values(3, 'Beef')""")
    cursor.execute("""insert into PizzaToppings values(4, 'Cheese')""")
    cursor.execute("""insert into PizzaToppings values(5, 'Chicken')""")
    cursor.execute("""insert into PizzaToppings values(6, 'Mushrooms')""")
    cursor.execute("""insert into PizzaToppings values(7, 'Onions')""")
    cursor.execute("""insert into PizzaToppings values(8, 'Pepperoni')""")
    cursor.execute("""insert into PizzaToppings values(9, 'Peppers')""")
    cursor.execute("""insert into PizzaToppings values(10, 'Salami')""")
    cursor.execute("""insert into PizzaToppings values(11, 'Tomatoes')""")
    cursor.execute("""insert into PizzaToppings values(12, 'Tomato Sauce')""")

    cursor.execute("""drop procedure if exists Statistics""")
    cursor.execute("""
                    Create procedure Statistics(num int, dat date)
                    deterministic
                    BEGIN
                        if num = 1 then # returns amount of orders by each hour, doesnt sort by days
                            select count(OrderId) as "amount of orders", hour(OrderTime) as "Hour"
                            from CustomersOrders
                            group by hour(OrderTime)
                            order by hour(OrderTime) asc;
                        elseif num = 2 then # returns amount of orders per each day
                            select count(OrderId) as "amount of orders", dayofweek(OrderTime) as "Day"
                            from CustomersOrders
                            group by dayofweek(OrderTime)
                            order by dayofweek(OrderTime) asc;
                        elseif num = 3 then # returns amount of orders per hour for a specific day 
                            select count(orderId) as "amount of orders", hour(OrderTime) as "Hour"
                            from CustomersOrders
                            where date(OrderTime) = dat
                            group by hour(OrderTime)
                            order by hour(OrderTime) asc;
                        else #returns amount of pizzas sold
                            select count(CustomerId), PizzaName
                            from (CustomersOrders inner join PizzaNames on CustomersOrders.PizzaId=PizzaNames.PizzaId)
                            group by PizzaNames.PizzaId
                            order by count(CustomerId) asc;
                        end if;
                        END""")

    cursor.execute("""drop trigger if exists UpdateRatingAfterUpdate""")
    cursor.execute("""
                    create trigger UpdateRatingAfterUpdate
                    after update 
                    on RunnersOrders for each ROW
                    begin
                        update Deliverymen D join (select RunnerId, avg(Rating) as aRating from RunnersOrders group by RunnerId) RO
                        on RO.RunnerId = D.RunnerId
                        set D.Rating = aRating
                        where D.RunnerId = new.RunnerId;
                    end""")

    cursor.execute("""drop procedure if exists changeRating""")
    cursor.execute("""
                    create procedure changeRating(newRating int, OrderID int)
                    deterministic
                    begin
                        update RunnersOrders
                        set RunnersOrders.Rating = newRating
                        where RunnersOrders.OrderId = OrderID;
                    end""")

    cursor.execute("""drop procedure if exists cancelOrder""")
    cursor.execute("""
                    create procedure cancelOrder(OrderID int, CancellationDesc varchar(30))
                    deterministic
                    begin
                        update RunnersOrders
                        set RunnersOrders.Cancellation = CancellationDesc
                        where RunnersOrders.OrderId = OrderID;
                    end""")

    cursor.execute("""drop procedure if exists pizzaInfo""")
    cursor.execute("""
                    create procedure pizzaInfo()
                    deterministic
                    begin
                        select pizzaId, group_concat(distinct ToppingName) as Toppings
                        from PizzaRecipes pr
                        inner join PizzaToppings pt
                        on pt.ToppingId = pr.ToppingId
                        group by pr.pizzaId;
                    end""")

    cursor.execute("""drop procedure if exists Payment""")
    cursor.execute("""
                    create procedure Payment()
                    deterministic
                    begin
                        select sum(case 
                                        when PizzaId = 1 then 12
                                        when PizzaId = 2 then 10
                                        end) into @PizzaEarnings 
                                        from CustomersOrders;

                        select sum(case 
                                    when Distance is not null then Distance*0.3 end) into @RunnerCost
                                    from RunnersOrders;

                        select (@PizzaEarnings - @RunnerCost) as earnings;

                    end""")

    cursor.execute("""drop procedure if exists newOrder""")
    cursor.execute("""
                    create procedure newOrder(OrderId int, newPickUpDate timestamp, Distance float, Duration int, Rating int, numb int, exper bool)
                    deterministic
                    this_proc:begin    
                        # if we add a new runner they will never be picked. 
                        # we should get all the Runners into latest delivery alternative in a separate table
                        # then when we get a runner with a low rating < 3 we can pick a runner from there instead. to prevent starvation
                        # We could also optimistically pick a new runner who haven't delivered any order yet by just picking a random int i python
                        # and make it like 20% chance to pick a new runner then we check here whether we have a new runner otherwise continue on.

                        #select a runner who haven't delivered anything yet
                        select RunnerId into @pickedRunner from Deliverymen D where D.RunnerId not in (select Ro.RunnerId from RunnersOrders Ro);

                        # if we are asked to find a deliver who haven't delivered anything yet and we can't pick the best runner
                        if numb=2 and @pickedRunner is null then
                            set @run = 1;
                        end if;

                        if numb=1 then #pick the best runner that is available
                            # find the latest delivery for each runner
                            drop table if exists recentPickup;
                            create temporary table recentPickup
                            select RunnerId, MAX(pickupTime) as mRP from RunnersOrders where Cancellation is null group by RunnerId;

                            # check if they are available for this delivery
                            drop table if exists availableRunners;
                            create temporary table availableRunners
                            select RunnerId from recentPickup where date_add(mRP, interval duration minute) < newPickUpDate; 

                            # of the available drivers find the one with the highest rating
                            select aR.RunnerId into @pickedRunner from availableRunners as aR inner join Deliverymen as D on aR.RunnerId = D.RunnerId order by Rating desc limit 1;		
                        elseif numb=3 then # pick the runner with the lowest score while still having a score greater than 3
                            drop table if exists recentPickup;
                            create temporary table recentPickup
                            select RunnerId, MAX(pickupTime) as mRP from RunnersOrders where Cancellation is null group by RunnerId;

                            # check if they are available for this delivery
                            drop table if exists availableRunners;
                            create temporary table availableRunners
                            select RunnerId from recentPickup where date_add(mRP, interval duration minute) < newPickUpDate; 

                            # of the available drivers find the one with the highest rating
                            select aR.RunnerId into @pickedRunner from availableRunners as aR inner join Deliverymen as D on aR.RunnerId = D.RunnerId where Rating > 3 order by Rating asc limit 1;		
                        elseif numb=4 then
                            drop table if exists recentPickup;
                            create temporary table recentPickup
                            select RunnerId, MAX(pickupTime) as mRP from RunnersOrders where Cancellation is null group by RunnerId;

                            # check if they are available for this delivery
                            drop table if exists availableRunners;
                            create temporary table availableRunners
                            select RunnerId from recentPickup where date_add(mRP, interval duration minute) < newPickUpDate; 

                            # of the available drivers find the one with the highest rating
                            select aR.RunnerId into @pickedRunner from availableRunners as aR inner join Deliverymen as D on aR.RunnerId = D.RunnerId where Rating <= 3 order by Rating asc limit 1;		
                        else
                            drop table if exists recentPickup;
                            create temporary table recentPickup
                            select RunnerId, MAX(pickupTime) as mRP from RunnersOrders where Cancellation is null group by RunnerId;

                            # check if they are available for this delivery
                            drop table if exists availableRunners;
                            create temporary table availableRunners
                            select RunnerId from recentPickup where date_add(mRP, interval duration minute) < newPickUpDate; 

                            # of the available drivers find the one with the highest rating
                            select aR.RunnerId into @pickedRunner from availableRunners as aR inner join Deliverymen as D on aR.RunnerId = D.RunnerId order by Rating asc limit 1;		
                        end if;

                        if @pickedRunner is null then
                            select "Couldn't find a runner this time";
                            leave this_proc;
                        end if;

                        # place a new order with that runner
                        if exper = 0 then
                            insert into RunnersOrders values(OrderId, @pickedRunner, newPickUpDate, Distance, Duration, null, Rating);
                        else
                            insert into RunnersOrders values(OrderId, @pickedRunner, newPickUpDate, Distance, Duration, null, Rating, null);
                        end if;
                    end""")


clear = lambda: os.system('cls')
OrdId = 12


def main():
    running = True
    while running:
        clear()
        print("hello, what would you like to do?")
        print("1. Customer")
        print("2. Administration")
        print("3. Experimental")
        print("4. exit")
        choice = int(input())

        if choice == 1:
            CustomerScreen()
        elif choice == 2:
            AdministratorScreen()
        elif choice == 3:
            Experimental()
        else:
            clear()
            running = False


def CustomerScreen():
    clear()
    print("1. Place new Order")
    print("2. Update order")
    print("3. View menu")
    print("4. return")
    choice = int(input())
    if choice == 1:
        MakeOrder()
    elif choice == 2:
        UpdateOrder()
    elif choice == 3:
        cursor.callproc("pizzaInfo")

        for result in cursor.stored_results():
            for row in result:
                print(row)

        input("Press enter to go back")
    return


def MakeOrder():
    clear()
    # optimistically pick a new runner who haven't delivered anything yet in hope of them becoming the number 1.
    # with a 20% chance of this happening here and a chance of it happening in the sql if we get a best runner with a score of
    # less than 3.

    # 50% pick the best runner
    # 20% chance to pick a new runner who haven't delivered anything yet
    # 15% to pick the runner with the lowest rating while rating > 3
    # 10% chance we pick a runner with a low rating
    # 5% chance we pick the runner with the lowest rating

    possibility = random.randint(0, 19)
    if possibility < 10:
        numb = 1
    elif 10 <= possibility < 14:
        numb = 2
    elif 14 <= possibility < 17:
        numb = 3
    elif 17 <= possibility < 19:
        numb = 4
    else:
        numb = 5

    # get a unique orderId
    global OrdId
    orderId = OrdId
    OrdId += 1

    customerId = int(input("What is your customer id?"))

    pizzaId = int(input("Which pizza do you want?"))

    exclusions = input("Any exclusions?")

    if not exclusions:
        exclusions = "''"

    extras = input("Any extras?")

    if not extras:
        extras = "''"

    orderTime = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    pickUpTime = input("PickUpTime: ")

    distance = int(input("What is the distance? "))

    duration = int(input("How long did it take to deliver? "))

    rating = input("Rating? ")

    if not rating:
        rating = None
    else:
        rating = int(rating)

    cursor.execute("""insert into CustomersOrders values({0}, {1}, {2}, {3}, {4}, '{5}')""".format(orderId, customerId, pizzaId,
                                                                                      exclusions, extras,
                                                                                      orderTime))
    clear()
    print(f"Order made!\nYour orderid is {orderId}")

    cursor.callproc("newOrder", [orderId, pickUpTime, distance, duration, rating, numb, experimental])

    if experimental:
        cursor.execute(f"""select price from RunnersOrders where OrderId = {orderId}""")
        print(cursor.fetchall())

    input("Press enter to go back")

    return


def UpdateOrder():
    clear()
    print("1. Update rating")
    print("2. Cancel order")
    print("3. return")

    choice = int(input())

    if choice == 1:
        clear()
        orderId = int(input("What is the orderId? "))
        rating = int(input("What is the new Rating? "))
        cursor.execute("""update RunnersOrders
                          set Rating = {0}
                          where OrderID = {1}""".format(rating, orderId))
    elif choice == 2:
        orderId = int(input("What is the orderId?"))
        reason = input("What is the reason?")
        cursor.execute("""update RunnersOrders
                          set Cancellation = "{0}"
                          where OrderID = {1};""".format(reason, orderId))
    return


def AdministratorScreen():
    clear()
    print("1. View statistics")
    print("2. Add new worker")
    print("3. Add new pizza")
    print("4. Calculate earnings")
    print("5. View Deliverymen")
    print("6. return")

    choice = int(input())

    if choice == 1:
        StatisticMenu()
    elif choice == 2:
        AddNewWorker()
    elif choice == 3:
        AddNewPizza()
    elif choice == 4:
        cursor.callproc("Payment")
        for result in cursor.stored_results():
            for row in result:
                print(row)
        input("Press enter to go back")
    elif choice == 5:
        cursor.execute("select * from Deliverymen")
        for result in cursor.fetchall():
            print(result)
        input("Press enter to go back")
    return


def StatisticMenu():
    clear()
    print("1. View amount of orders per hour")
    print("2. View amount of orders per day")
    print("3. View amount of orders for a specific date")
    print("4. View amount sold of each pizza")
    print("5. Return")

    choice = int(input("Choice: "))

    if 0 < choice < 4:
        date = input("Date: ")
        cursor.callproc("Statistics", [choice, date])
        for result in cursor.stored_results():
            for row in result:
                print(row)

    elif choice == 4:
        cursor.callproc("Statistics", [choice, None])
        for result in cursor.stored_results():
            for row in result:
                print(row)

    input("Press enter to go back")
    return


def AddNewWorker():
    clear()
    workerId = int(input("What is the workers id? "))
    name = input("What is the workers name? ")

    cursor.execute("""insert into Deliverymen values({0}, '{1}', curdate(), null)""".format(workerId, name))


def AddNewPizza():
    clear()
    pizzaId = int(input("What number"))
    toppings = input("Toppings, comma seperated")

    toppings.split(',')

    for num in toppings:
        cursor.execute("""insert into PizzaRecipes values({0}, {1})""".format(pizzaId, int(num)))

    return


if __name__ == "__main__":
    Start()
    main()
