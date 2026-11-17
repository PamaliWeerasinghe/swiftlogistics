import React, { useEffect, useRef, useState } from 'react';
import L from 'leaflet';

interface LiveDeliveryMapProps {
    orderId: string;
    status: string;
    originLat?: number;
    originLng?: number;
    destLat?: number;
    destLng?: number;
    address: string;
}

export const LiveDeliveryMap: React.FC<LiveDeliveryMapProps> = ({
                                                                    orderId,
                                                                    status,
                                                                    originLat = 6.9388,
                                                                    originLng = 79.8745,
                                                                    destLat = 6.9016,
                                                                    destLng = 79.8654,
                                                                    address,
                                                                }) => {
    const mapContainerRef = useRef<HTMLDivElement | null>(null);
    const mapInstanceRef = useRef<L.Map | null>(null);
    const vanMarkerRef = useRef<L.CircleMarker | null>(null);
    const travelledLineRef = useRef<L.Polyline | null>(null);
    const [progress, setProgress] = useState<number>(status === 'DELIVERED' ? 100 : 0);

    // Initialize Leaflet Map once
    useEffect(() => {
        if (!mapContainerRef.current || mapInstanceRef.current) return;

        const map = L.map(mapContainerRef.current, {
            center: [(originLat + destLat) / 2, (originLng + destLng) / 2],
            zoom: 13,
            zoomControl: false,
        });

        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; OpenStreetMap',
        }).addTo(map);

        // Warehouse Origin Marker
        L.circleMarker([originLat, originLng], {
            radius: 7,
            color: '#38bdf8',
            fillColor: '#0284c7',
            fillOpacity: 1,
        })
            .bindPopup('🏭 SwiftLogistics Central Warehouse')
            .addTo(map);

        // Destination Marker
        L.circleMarker([destLat, destLng], {
            radius: 7,
            color: '#f43f5e',
            fillColor: '#e11d48',
            fillOpacity: 1,
        })
            .bindPopup(`📍 Drop-off: ${address}`)
            .addTo(map);

        // Planned Route Dashed Line
        L.polyline(
            [
                [originLat, originLng],
                [destLat, destLng],
            ],
            { color: '#475569', weight: 3, dashArray: '6, 6' }
        ).addTo(map);

        // Travelled Solid Line
        travelledLineRef.current = L.polyline([[originLat, originLng]], {
            color: '#10b981',
            weight: 4,
        }).addTo(map);

        // Live Moving Delivery Van Marker
        vanMarkerRef.current = L.circleMarker([originLat, originLng], {
            radius: 9,
            color: '#ffffff',
            weight: 2,
            fillColor: '#10b981',
            fillOpacity: 1,
        })
            .bindPopup(`🚚 Active Van (${orderId})`)
            .addTo(map);

        mapInstanceRef.current = map;

        return () => {
            map.remove();
            mapInstanceRef.current = null;
        };
    }, [originLat, originLng, destLat, destLng, address, orderId]);

    // Animate Van Movement when Driver starts transit ("IN_TRANSIT")
    useEffect(() => {
        if (status === 'DELIVERED') {
            setProgress(100);
            vanMarkerRef.current?.setLatLng([destLat, destLng]);
            travelledLineRef.current?.setLatLngs([
                [originLat, originLng],
                [destLat, destLng],
            ]);
            return;
        }

        if (status !== 'IN_TRANSIT') return;

        const timer = setInterval(() => {
            setProgress((prev) => {
                const next = prev >= 95 ? 95 : prev + 5; // Advances toward destination until Delivered is clicked
                const ratio = next / 100;
                const currentLat = originLat + (destLat - originLat) * ratio;
                const currentLng = originLng + (destLng - originLng) * ratio;

                vanMarkerRef.current?.setLatLng([currentLat, currentLng]);
                travelledLineRef.current?.setLatLngs([
                    [originLat, originLng],
                    [currentLat, currentLng],
                ]);
                mapInstanceRef.current?.panTo([currentLat, currentLng], { animate: true });

                // Push live GPS telemetry through API Gateway (:8000)
                fetch(`http://localhost:8000/api/driver/manifests/${orderId}/location`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ lat: currentLat, lng: currentLng }),
                }).catch(() => {});

                return next;
            });
        }, 1500);

        return () => clearInterval(timer);
    }, [status, orderId, originLat, originLng, destLat, destLng]);

    return (
        <div className="space-y-1.5">
            <div className="flex items-center justify-between text-[11px] text-slate-400">
        <span>
          {status === 'ASSIGNED' && '🏭 Parked at Warehouse Bay'}
            {status === 'IN_TRANSIT' && `🚚 En Route to Destination (${progress}%)`}
            {status === 'DELIVERED' && '✅ Arrived at Destination (100%)'}
            {status === 'FAILED_ATTEMPT' && '⚠️ Transit Halted at Stop'}
        </span>
                <span className="font-mono text-emerald-400">GPS LIVE</span>
            </div>
            <div
                ref={mapContainerRef}
                className="w-full h-44 rounded-xl overflow-hidden border border-slate-800 z-0"
            />
        </div>
    );
};