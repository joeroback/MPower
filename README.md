# MPower

Convert BMW MPower files from BMW M Laptimer to CSV's in Harry's Laptimer format, compatiable with Race Render 3 and Telemetry Overlay.

## Decoding MPower Files

The exported mpower files are in fact zip archives. Each archive consists of many `.far` files, some `.json` files, and a PNG file. What data is in each `.far` file seems to vary from vehicle model to vehicle model.

Far files from an iPhone 13 Pro running iOS 15 on a 2022 BMW M3 Competition xDrive contain:

    AccelerationLateral.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Lateral Gs -> 8 bytes, float (g)

    AccelerationLongitudinal.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Longitudinal Gs -> 8 bytes, float (g)

    AcceleratorPedal.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Throttle -> 8 bytes, float (% 0.0 to 100.0)

    BrakeContact.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Brake Pressed -> 8 bytes, float (0 or 1.0)

    CurrentConsumption.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Current MPG -> 8 bytes, float

    Distance.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Distance Traveled -> 8 bytes, float (meters)

    Gear.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            ?? -> 8 bytes

    Gearbox.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Mode -> 8 bytes, integer # todo detail modes
            Gear -> 8 bytes, integer # todo detail values

    Heading.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Heading -> 8 bytes, float, degrees

    Location.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Latitude -> 8 bytes, float, degrees
            Longitude -> 8 bytes, float, degrees

    RPM.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            RPM -> 8 bytes, float

    Speed.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Speed -> 8 bytes, float, meters per second

    Steering.far:
        Record Size -> 8 bytes, integer
        Record:
            Time -> 8 bytes, float, epoch in iOS timezone
            Steering Wheel Angle -> 8 bytes, float, degrees
